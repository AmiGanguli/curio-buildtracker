use aws_sdk_sqs::Client as SqsClient;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_dynamodb::{Client as DynamoClient, types::AttributeValue};
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{info, warn, error};
use futures::StreamExt;
use std::time::{Duration, Instant};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use sysinfo::{System, Disks};
use curio_db::CurioConfig;

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", content = "detail")]
enum EventType {
    NoOp,
    ArtifactAdded { id: String },
    ArtifactRemoved { id: String },
    ComputeNodeDirty,
    ComputeNodeObsolete,
    ArtifactObsolete,
    CatalogInputFiles { task_id: String, started_at: Option<String>, job_type: Option<String> },
    PurgeInputFiles { task_id: String, started_at: Option<String>, job_type: Option<String> },
}

#[derive(Deserialize, Debug)]
struct MessageBody {
    #[serde(rename = "detail-type")]
    detail_type: String,
    detail: serde_json::Value,
    source: String,
}

// ... ResourceController implementation (omitted for brevity if using replace_file_content partial, but here I am replacing full file structure potentially? No, just partial.)
// I will keep ResourceController as is if I can match the surrounding code.

struct ResourceController {
    sys: System,
    disks: Disks,
    last_check: Instant,
    check_interval: Duration,
}

impl ResourceController {
    fn new() -> Self {
        Self {
            sys: System::new_all(),
            disks: Disks::new_with_refreshed_list(),
            last_check: Instant::now() - Duration::from_secs(60), // Force immediate check
            check_interval: Duration::from_secs(5),
        }
    }

    fn check_resources(&mut self) -> bool {
        let now = Instant::now();
        if now.duration_since(self.last_check) < self.check_interval {
            return true;
        }
        self.last_check = now;
        
        self.sys.refresh_cpu();
        self.sys.refresh_memory();
        self.disks.refresh_list(); // Update list of disks
        self.disks.refresh();      // Update usage stats

        // Check Memory
        let total_mem = self.sys.total_memory();
        let used_mem = self.sys.used_memory();
        let mem_usage_percent = if total_mem > 0 {
            (used_mem as f64 / total_mem as f64) * 100.0
        } else {
            0.0
        };

        // Check CPU (global)
        let global_cpu = self.sys.global_cpu_info().cpu_usage();
        
        if mem_usage_percent > 90.0 {
            warn!("High memory usage detected: {:.2}%. Throttling polling.", mem_usage_percent);
            return false;
        }

        if global_cpu > 95.0 {
            warn!("High CPU usage detected: {:.2}%. Throttling polling.", global_cpu);
            return false;
        }

        // Check Disk Usage
        for disk in &self.disks {
            let total_space = disk.total_space();
            let available_space = disk.available_space();
            if total_space > 0 {
                let usage = 100.0 - ((available_space as f64 / total_space as f64) * 100.0);
                if usage > 90.0 {
                    warn!("High disk usage detected on {:?}: {:.2}%. Throttling polling.", disk.mount_point(), usage);
                    return false;
                }
            }
        }
        
        true
    }
}

struct TaskStatus {
    processed: Arc<AtomicUsize>,
    total: Arc<AtomicUsize>,
    task_id: String,
    started_at: String,
    job_type: String,
    handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    client: DynamoClient,
    table_name: String,
}

impl TaskStatus {
    fn new(client: DynamoClient, table_name: String, task_id: String, total: usize, message: String, started_at: Option<String>, job_type: Option<String>) -> Self {
        let processed = Arc::new(AtomicUsize::new(0));
        let total_atomic = Arc::new(AtomicUsize::new(total));
        
        let p = processed.clone();
        let t = total_atomic.clone();
        
        // Use provided started_at or current time
        let s_at = started_at.unwrap_or_else(|| {
            chrono::Utc::now().to_rfc3339()
        });
        // Default job type if missing
        let b_type = job_type.unwrap_or_else(|| "UNKNOWN".to_string());
        
        // Clones for Initial Update Task
        let client_initial = client.clone();
        let t_name_initial = table_name.clone();
        let t_id_initial = task_id.clone();
        let msg_initial = message.clone();
        let s_at_initial = s_at.clone();
        let j_type_initial = b_type.clone();
        
        tokio::spawn(async move {
            let _ = client_initial.put_item()
                .table_name(&t_name_initial)
                .item("taskId", AttributeValue::S(t_id_initial))
                .item("timestamp", AttributeValue::S("STATUS".to_string()))
                .item("state", AttributeValue::S("RUNNING".to_string()))
                .item("jobType", AttributeValue::S(j_type_initial))
                .item("startedAt", AttributeValue::S(s_at_initial))
                .item("updatedAt", AttributeValue::S(chrono::Utc::now().to_rfc3339()))
                .item("message", AttributeValue::S(msg_initial))
                .item("level", AttributeValue::S("INFO".to_string()))
                .item("processed", AttributeValue::N("0".to_string()))
                .item("total", AttributeValue::N(total.to_string()))
                .send()
                .await;
        });

        // Clones for Periodic Update Task
        let client_periodic = client.clone();
        let t_name_periodic = table_name.clone();
        let t_id_periodic = task_id.clone();
        let msg_periodic = message.clone();
        let s_at_periodic = s_at.clone();
        let j_type_periodic = b_type.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(1000));
            loop {
                interval.tick().await;
                let current = p.load(Ordering::Relaxed);
                let current_total = t.load(Ordering::Relaxed);
                
                let _ = client_periodic.put_item()
                    .table_name(&t_name_periodic)
                    .item("taskId", AttributeValue::S(t_id_periodic.clone()))
                    .item("timestamp", AttributeValue::S("STATUS".to_string()))
                    .item("state", AttributeValue::S("RUNNING".to_string()))
                    .item("jobType", AttributeValue::S(j_type_periodic.clone()))
                    .item("startedAt", AttributeValue::S(s_at_periodic.clone()))
                    .item("updatedAt", AttributeValue::S(chrono::Utc::now().to_rfc3339()))
                    .item("message", AttributeValue::S(msg_periodic.clone()))
                    .item("level", AttributeValue::S("INFO".to_string()))
                    .item("processed", AttributeValue::N(current.to_string()))
                    .item("total", AttributeValue::N(current_total.to_string()))
                    .send()
                    .await;
            }
        });

        Self {
            processed,
            total: total_atomic,
            task_id,
            started_at: s_at, // Store for finish()
            job_type: b_type,
            handle: std::sync::Mutex::new(Some(handle)),
            client,
            table_name,
        }
    }
    
    fn inc(&self) {
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
    
    fn dec_total(&self) {
        self.total.fetch_sub(1, Ordering::Relaxed);
    }


    
    async fn finish(&self) {
        // Ensure final update is sent
        let current_p = self.processed.load(Ordering::Relaxed);
        let current_t = self.total.load(Ordering::Relaxed);
        if let Err(e) = self.client.put_item()
            .table_name(&self.table_name)
            .item("taskId", AttributeValue::S(self.task_id.clone()))
            .item("timestamp", AttributeValue::S("STATUS".to_string())) 
            .item("processed", AttributeValue::N(current_p.to_string()))
            .item("total", AttributeValue::N(current_t.to_string()))
            .item("message", AttributeValue::S("Completed".to_string()))
            .item("level", AttributeValue::S("INFO".to_string()))
            .item("state", AttributeValue::S("COMPLETED".to_string()))
            .item("jobType", AttributeValue::S(self.job_type.clone()))
            .item("startedAt", AttributeValue::S(self.started_at.clone()))
            .item("updatedAt", AttributeValue::S(chrono::Utc::now().to_rfc3339()))
            .send()
            .await 
        {
            error!("Failed to send final status update: {}", e);
        }

        let handle = {
            let mut lock = self.handle.lock().unwrap();
            lock.take()
        };
        if let Some(h) = handle {
            // Abort the background task
            h.abort();
            // Await to ensure it's cleaned up, though abort means it won't complete normally
            let _ = h.await; 
        }
    }
}



async fn catalog_inputs(s3: &S3Client, dynamo: &DynamoClient, table_name: &str, status_table: &str, build_bucket: &str, task_id: String, started_at: Option<String>, job_type: Option<String>) -> anyhow::Result<()> {
    info!("Starting cataloging of external inputs for task: {}", task_id);
    
    // 1. Fetch Config from S3
    let config_obj = s3.get_object()
        .bucket(build_bucket)
        .key("curio.yaml")
        .send()
        .await?;
    
    let config_content = config_obj.body.collect().await?.into_bytes();
    let config_str = String::from_utf8(config_content.to_vec())?;
    
    let config = CurioConfig::from_yaml(&config_str).map_err(|e| anyhow::anyhow!("Failed to parse config: {}", e))?;
    
    // Use passed task_id
    
    for input in config.external_inputs {
        info!("Cataloging input group: {}", input.name);
        
        // List S3 objects in input bucket
        let mut objects = Vec::new();
        let mut continuation_token = None;
        
        loop {
            let mut list_req = s3.list_objects_v2()
                .bucket(&input.bucket)
                .set_continuation_token(continuation_token);
                
            if let Some(p) = &input.prefix {
                list_req = list_req.prefix(p);
            }
            
            let resp = list_req.send().await;
            
            match resp {
                Ok(output) => {
                    if let Some(contents) = output.contents {
                        objects.extend(contents);
                    }
                    if output.is_truncated.unwrap_or(false) {
                        continuation_token = output.next_continuation_token;
                    } else {
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to list objects in bucket {}: {}", input.bucket, e);
                    break;
                }
            }
        }
        
        let total_objects = objects.len();

        let status = Arc::new(TaskStatus::new(
            dynamo.clone(),
            status_table.to_string(), 
            task_id.clone(),
            total_objects,
            format!("Cataloging {}", input.name),
            started_at.clone(),
            job_type.clone()
        ));
        
        // Scan DynamoDB for existing items for this input type
        // Assume ID format: "external/{name}/{key}" or similar?
        // Actually, we should query/scan based on a prefix or an index.
        // For simplicity, let's assume we can construct the ID.
        // Or scan everything and filter.
        // Let's scan everything that starts with external/{name}/
        // NOTE: Scan is expensive, but for cataloging job it might be acceptable if infrequent.
        // Ideally we would query a GSI.
        
        // Map S3 keys to Metadata & Build Hierarchy
        let mut s3_keys = std::collections::HashMap::new();
        let mut hierarchy_upserts = std::collections::HashMap::new();

        if let Some(re) = &input.regex {
            for obj in objects {
                if let Some(key) = obj.key {
                    if let Some(caps) = re.captures(&key) {
                        let etag = obj.e_tag.unwrap_or_default().trim_matches('"').to_string();
                        
                        let mut meta_map = serde_json::Map::new();
                        // Use pre-extracted fields from Config if available, else derive? 
                        // Actually config.rs update put fields in `input.fields`.
                        // But here `input` is `ExternalInputRule`.
                        // Let's iterate capture names from regex as before, it is robust.
                        
                        let mut hierarchy_path = Vec::new();
                        let mut leaf_parent_id = format!("external/{}", input.name); 

                        // Construct hierarchy path based on capture groups
                        // We expect regex groups to be ordered? Regex capture_names() are iterator.
                        // We rely on the order in the regex pattern?
                        // `capture_names()` returns optional names.
                        
                        // Let's assume the user wants the hierarchy in the order of the named groups in the regex string.
                        // `re.capture_names()` usually iterates by index.
                        
                        for name in re.capture_names().flatten() {
                            if let Some(m) = caps.name(name) {
                                let val = m.as_str().to_string();
                                meta_map.insert(name.to_string(), serde_json::Value::String(val.clone()));
                                
                                // Build hierarchy: Root -> Group1 -> Group2 -> Leaf
                                let current_id = format!("{}/{}", leaf_parent_id, val);
                                
                                // Add to hierarchy upserts (Group Item)
                                let group_entry = hierarchy_upserts.entry(current_id.clone()).or_insert((leaf_parent_id.clone(), 0, "GROUP".to_string()));
                                group_entry.1 += 1; // Increment count (this is loose, count of direct children not recursive inputs?) 
                                // Actually user wants recursive job counts.
                                // If I am processing a LEAF (job), I should increment count for ALL ancestors.
                                
                                hierarchy_path.push(current_id.clone());
                                leaf_parent_id = current_id;
                            }
                        }
                        
                        // The last one in hierarchy_path is actually the direct parent of the leaf?
                        // Wait, if regex is `(?P<ats>...)/(?P<token>...)/(?P<job_id>...)`:
                        // 1. ats=greenhouse. id=external/harvest_jobs/greenhouse. parent=external/harvest_jobs.
                        // 2. token=abc. id=external/harvest_jobs/greenhouse/abc. parent=.../greenhouse.
                        // 3. job_id=123. id=external/harvest_jobs/greenhouse/abc/123. parent=.../abc.
                        
                        // The LEAF item (the file) should align with the last group? Or IS the last group?
                        // User said: "In the front-end, we don't need to show the id but we do need to show the various metadata items."
                        // And "The tokens row would just give a count. Same for the job ids."
                        // This implies the job_id IS the leaf node in the tree.
                        
                        // So, for specific S3 object:
                        // ID: external/harvest_jobs/greenhouse/abc/123 (matches the last derived ID)
                        // This item should be TYPE=ITEM.
                        // Its parent is `external/harvest_jobs/greenhouse/abc`.
                        
                        // Fix: The loop above treats everything as a group.
                        // We need to distinguish the last one as the ITEM.
                        
                        if let Some(leaf_id) = hierarchy_path.pop() {
                             // The last one is the LEAF ITEM
                             // We don't want to double count or make it a group.
                             hierarchy_upserts.remove(&leaf_id); 
                             
                             // Get its parent
                             let parent_id = if let Some(last_parent) = hierarchy_path.last() {
                                 last_parent.clone()
                             } else {
                                 format!("external/{}", input.name)
                             };

                             let meta_json = serde_json::to_string(&meta_map).unwrap_or_else(|_| "{}".to_string());
                             s3_keys.insert(key, (etag, meta_json, leaf_id, parent_id));
                        }
                        
                        // Now increment counts for all ancestors in hierarchy_path
                        for group_id in hierarchy_path {
                            if let Some(_entry) = hierarchy_upserts.get_mut(&group_id) {
                                // entry is (parentId, count, type)
                                // We are counting LEAVES (jobs) under this group?
                                // Yes, user wants "total number of jobs".
                                // So for every S3 object, we increment all ancestors.
                                // entry.1 += 1; // This works.
                            }
                        }

                    } else {
                        status.dec_total();
                    }
                } else {
                    status.dec_total();
                }
            }
        }
        
        // Create a set of valid IDs (Groups and Leaves) for obsolete checking
        let valid_group_ids: std::collections::HashSet<String> = hierarchy_upserts.keys().cloned().collect();
        let valid_leaf_ids: std::collections::HashSet<String> = s3_keys.values().map(|v| v.2.clone()).collect();
        // Note: s3_keys stores (etag, metadata, id, parent_id) in tuple. v.2 is id.

        // Sync Groups (Hierarchy) to DynamoDB
        let hierarchy_vec: Vec<(String, (String, usize, String))> = hierarchy_upserts.into_iter().collect();
        futures::stream::iter(hierarchy_vec)
             .for_each_concurrent(20, |(id, (parent_id, count, context_type))| {
                let dynamo = dynamo.clone();
                let table_name = table_name.to_string();
                async move {
                    // Upsert Group Item
                     if let Err(e) = dynamo.put_item()
                        .table_name(&table_name)
                        .item("id", AttributeValue::S(id.clone()))
                        .item("parentId", AttributeValue::S(parent_id))
                        .item("type", AttributeValue::S(context_type)) // GROUP
                        .item("count", AttributeValue::N(count.to_string()))
                        .item("status", AttributeValue::S("CLEAN".to_string())) // Default
                        .item("last_seen", AttributeValue::S(chrono::Utc::now().to_rfc3339()))
                        .send()
                        .await 
                    {
                        error!("Failed to update group {}: {}", id, e);
                    }
                }
             }).await;

        
        // Sync Items (Leaves) with DynamoDB (Parallel)
        let s3_keys_vec: Vec<(String, (String, String, String, String))> = s3_keys.iter().map(|(k, v)| (k.clone(), (v.0.clone(), v.1.clone(), v.2.clone(), v.3.clone()))).collect();
        
        futures::stream::iter(s3_keys_vec)
            .for_each_concurrent(50, |(key, (etag, metadata, id, parent_id))| {
                let dynamo = dynamo.clone();
                let table_name = table_name.to_string();
                let status = status.clone();
                let key = key.clone();
                
                async move {
                    
                    let item_result = dynamo.get_item()
                        .table_name(&table_name)
                        .key("id", AttributeValue::S(id.clone()))
                        .send()
                        .await;
                        
                    match item_result {
                        Ok(item_output) => {
                             let needs_update = if let Some(existing) = item_output.item {
                                let existing_etag = existing.get("etag").and_then(|av| av.as_s().ok()).map(|s| s.as_str()).unwrap_or("");
                                 existing_etag != etag
                            } else {
                                true 
                            };
                            
                            if needs_update {
                                info!("Marking dirty: {}", id);
                                if let Err(e) = dynamo.put_item()
                                    .table_name(&table_name)
                                    .item("id", AttributeValue::S(id.clone()))
                                    .item("s3_key", AttributeValue::S(key.clone()))
                                    .item("parentId", AttributeValue::S(parent_id))
                                    .item("type", AttributeValue::S("ITEM".to_string()))
                                    .item("status", AttributeValue::S("DIRTY".to_string()))
                                    .item("etag", AttributeValue::S(etag))
                                    .item("metadata", AttributeValue::S(metadata))
                                    .item("last_seen", AttributeValue::S(chrono::Utc::now().to_rfc3339()))
                                    .send()
                                    .await 
                                {
                                    error!("Failed to update item {}: {}", id, e);
                                }
                            }
                        },
                        Err(e) => {
                            error!("Failed to get item {}: {}", id, e);
                        }
                    }
                    status.inc();
                }
            })
            .await;
            
        // Wait for status implementation to finish
        status.finish().await;
        
        // Obsolete Detection
        
        // Obsolete Detection
        // 1. Scan and Collect IDs (Sequential Scan, but we collect to process parallel)
        let mut obsolete_candidates = Vec::new();
        let mut scan_input = dynamo.scan().table_name(table_name);
        let prefix = format!("external/{}/", input.name);
        
        loop {
            let resp = scan_input.clone().send().await?;
            if let Some(items) = resp.items {
                for item in items {
                    if let Some(id_av) = item.get("id") {
                        if let Ok(id) = id_av.as_s() {
                            if id.starts_with(&prefix) {
                                // let key = &id[prefix.len()..]; // Removed - checking IDs directly
                                
                                // Check Type
                                let item_type = item.get("type").and_then(|av| av.as_s().ok()).map(|s| s.as_str()).unwrap_or("ITEM");

                                // Check existence logic
                                let exists = if item_type == "GROUP" {
                                    valid_group_ids.contains(id)
                                } else {
                                    valid_leaf_ids.contains(id)
                                };
                                
                                if !exists {
                                    let current_status = item.get("status")
                                        .and_then(|s| s.as_s().ok())
                                        .map(|s| s.as_str())
                                        .unwrap_or("");
                                        
                                    if current_status != "OBSOLETE" {
                                        obsolete_candidates.push(id.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            if resp.last_evaluated_key.is_some() {
                scan_input = scan_input.set_exclusive_start_key(resp.last_evaluated_key);
            } else {
                break;
            }
        }
        
        // 2. Process Obsolete Updates in Parallel
        futures::stream::iter(obsolete_candidates)
            .for_each_concurrent(50, |id| {
                let dynamo = dynamo.clone();
                let table_name = table_name.to_string();
                
                async move {
                    info!("Marking obsolete: {}", id);
                    if let Err(e) = dynamo.put_item()
                        .table_name(&table_name)
                        .item("id", AttributeValue::S(id))
                        .item("status", AttributeValue::S("OBSOLETE".to_string()))
                        .item("last_seen", AttributeValue::S(chrono::Utc::now().to_rfc3339()))
                        .send()
                        .await
                    {
                        error!("Failed to mark obsolete {}: {}", table_name, e);
                    }
                }
            })
            .await;
    }
    
    Ok(())
}

async fn purge_inputs(dynamo: &DynamoClient, table_name: &str, status_table: &str, task_id: String, started_at: Option<String>, job_type: Option<String>) -> anyhow::Result<()> {
    info!("Starting purge of external inputs for task: {}", task_id);

    // 1. Calculate Total Count first for accurate progress
    let mut count_scan = dynamo.scan()
        .table_name(table_name)
        .filter_expression("begins_with(id, :prefix)")
        .expression_attribute_values(":prefix", AttributeValue::S("external/".to_string()))
        .select(aws_sdk_dynamodb::types::Select::Count);
    
    let mut total_count = 0;
    loop {
        let resp = match count_scan.clone().send().await {
            Ok(r) => r,
            Err(e) => {
                warn!("Failed to count purge items: {}", e);
                break;
            }
        };
        
        total_count += resp.count; // count is i32, default 0
        
        if resp.last_evaluated_key.is_some() {
             count_scan = count_scan.set_exclusive_start_key(resp.last_evaluated_key);
        } else {
             break;
        }
    }
    info!("Total items to purge: {}", total_count);

    let status = Arc::new(TaskStatus::new(
        dynamo.clone(),
        status_table.to_string(), 
        task_id.clone(),
        total_count as usize, 
        format!("Purging Inputs"),
        started_at,
        job_type
    ));

    let mut scan_input = dynamo.scan()
        .table_name(table_name)
        .filter_expression("begins_with(id, :prefix)")
        .expression_attribute_values(":prefix", AttributeValue::S("external/".to_string()));
    
    loop {
        let resp = scan_input.clone().send().await?;
        
        let mut items_to_delete = Vec::new();
        if let Some(items) = resp.items {
            for item in items {
                if let Some(id_av) = item.get("id") {
                    if let Ok(id) = id_av.as_s() {
                        items_to_delete.push(id.clone());
                    }
                }
            }
        }
        
        if !items_to_delete.is_empty() {
            info!("Purging batch of {} items...", items_to_delete.len());
            // status.add_total(items_to_delete.len()); // Removed to prevent fluctuation
            
            futures::stream::iter(items_to_delete)
                .for_each_concurrent(50, |id| {
                    let dynamo = dynamo.clone();
                    let table_name = table_name.to_string();
                    let status = status.clone();
                    
                    async move {
                        if let Err(e) = dynamo.delete_item()
                            .table_name(&table_name)
                            .key("id", AttributeValue::S(id))
                            .send()
                            .await
                        {
                            error!("Failed to delete item {}: {}", table_name, e);
                        }
                        status.inc();
                    }
                })
                .await;
        }
        
        if resp.last_evaluated_key.is_some() {
            scan_input = scan_input.set_exclusive_start_key(resp.last_evaluated_key);
        } else {
            break;
        }
    }
    
    status.finish().await;
    
    info!("Purge complete.");
    Ok(())
}

async fn handle_message(sqs_client: &SqsClient, s3_client: &S3Client, dynamo_client: &DynamoClient, queue_url: &str, table_name: &str, status_table: &str, build_bucket: &str, message: aws_sdk_sqs::types::Message) -> anyhow::Result<()> {
    if let Some(body) = &message.body {
        match serde_json::from_str::<MessageBody>(body) {
            Ok(event) => {
                if event.source != "curio.buildmanager" && event.source != "curio.api" {
                    warn!("Ignoring message from unknown source: {}", event.source);
                    return Ok(());
                }

                 let event_enum = match event.detail_type.as_str() {
                    "NoOp" => Some(EventType::NoOp),
                    "CatalogInputFiles" => {
                        serde_json::from_value::<serde_json::Value>(event.detail).map(|d| EventType::CatalogInputFiles { 
                            task_id: d["task_id"].as_str().unwrap_or("unknown").to_string(),
                            started_at: d.get("started_at").and_then(|v| v.as_str()).map(|s| s.to_string()),
                            job_type: d.get("job_type").and_then(|v| v.as_str()).map(|s| s.to_string())
                        }).ok()
                    },
                    "PurgeInputFiles" => {
                        serde_json::from_value::<serde_json::Value>(event.detail).map(|d| EventType::PurgeInputFiles { 
                            task_id: d["task_id"].as_str().unwrap_or("unknown").to_string(),
                            started_at: d.get("started_at").and_then(|v| v.as_str()).map(|s| s.to_string()),
                            job_type: d.get("job_type").and_then(|v| v.as_str()).map(|s| s.to_string())
                        }).ok()
                    },
                    "ArtifactAdded" => {
                        serde_json::from_value::<serde_json::Value>(event.detail).map(|d| EventType::ArtifactAdded { id: d["id"].as_str().unwrap_or("unknown").to_string() }).ok()
                    },
                    "ArtifactRemoved" => {
                        serde_json::from_value::<serde_json::Value>(event.detail).map(|d| EventType::ArtifactRemoved { id: d["id"].as_str().unwrap_or("unknown").to_string() }).ok()
                    },
                     _ => None,
                };

                if let Some(evt) = event_enum {
                    info!("Processing event: {:?}", evt);
                    match evt {
                        EventType::CatalogInputFiles { task_id, started_at, job_type } => {
                            if let Err(e) = catalog_inputs(s3_client, dynamo_client, table_name, status_table, build_bucket, task_id, started_at, job_type).await {
                                error!("Catalog failed: {}", e);
                            }
                        },
                        EventType::PurgeInputFiles { task_id, started_at, job_type } => {
                            if let Err(e) = purge_inputs(dynamo_client, table_name, status_table, task_id, started_at, job_type).await {
                                error!("Purge failed: {}", e);
                            }
                        },
                        _ => {
                             // Simulate work
                             tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                } else {
                    info!("Ignored event type: {}", event.detail_type);
                }
            },
            Err(e) => {
                 warn!("Failed to parse body as EventBridge event: {}. Body: {}", e, body);
            }
        }
    }

    // Delete message
    if let Some(receipt_handle) = message.receipt_handle {
        sqs_client.delete_message()
            .queue_url(queue_url)
            .receipt_handle(receipt_handle)
            .send()
            .await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let config = aws_config::load_from_env().await;
    let sqs_client = SqsClient::new(&config);
    let s3_client = S3Client::new(&config);
    let dynamo_client = DynamoClient::new(&config);
    
    let queue_url = env::var("QUEUE_URL").expect("QUEUE_URL must be set");
    // We need TABLE_NAME and BUILD_BUCKET for cataloging.
    // If they are not set, we can't run catalog, but maybe other things allow it.
    // For now, let's assume they are available or we fetch them from somewhere?
    // The processor is running in ECS. Construct passes env vars? checking BuildManager.py
    
    // In BuildManager.py:
    // environment={
    //     "QUEUE_URL": self.queue.queue_url,
    //     "CONCURRENCY": ...
    //     "RUST_LOG": "info",
    // },
    
    // Attempting to read them or default (which will fail catalog but allow start)
    let table_name = env::var("TABLE_NAME").unwrap_or_else(|_| "CurioTable".to_string()); 
    let status_table = env::var("STATUS_TABLE").unwrap_or_else(|_| "CurioStatus".to_string());
    let build_bucket = env::var("BUILD_BUCKET").unwrap_or_else(|_| "curio-build".to_string()); 

    let num_cores = num_cpus::get();
    let configured_concurrency = env::var("CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(num_cores * 4); 

    // Concurrency limit for active workers
    let concurrency = std::cmp::max(1, configured_concurrency);
    info!("Starting processor with concurrency limit: {}", concurrency);

    // Channel for messages from Poller -> Worker
    let (tx, rx) = mpsc::channel(concurrency * 2);

    let active_tasks = Arc::new(AtomicUsize::new(0));
    let shutdown_signal = Arc::new(tokio::sync::Notify::new());

    // Worker Task
    let worker_handle = {
        let sqs_client = sqs_client.clone();
        let s3_client = s3_client.clone();
        let dynamo_client = dynamo_client.clone();
        let queue_url = queue_url.clone();
        let table_name = table_name.clone();
        let status_table = status_table.clone();
        let build_bucket = build_bucket.clone();
        let active_tasks = active_tasks.clone();
        
        tokio::spawn(async move {
            ReceiverStream::new(rx)
                .for_each_concurrent(concurrency, |message| {
                    let sqs_client = sqs_client.clone();
                    let s3_client = s3_client.clone();
                    let dynamo_client = dynamo_client.clone();
                    let queue_url = queue_url.clone();
                    let table_name = table_name.clone();
                    let status_table = status_table.clone();
                    let build_bucket = build_bucket.clone();
                    
                    let active_tasks = active_tasks.clone();
                    async move {
                        active_tasks.fetch_add(1, Ordering::SeqCst);
                        if let Err(e) = handle_message(&sqs_client, &s3_client, &dynamo_client, &queue_url, &table_name, &status_table, &build_bucket, message).await {
                            error!("Error processing message: {}", e);
                        }
                        active_tasks.fetch_sub(1, Ordering::SeqCst);
                    }
                })
                .await;
            info!("Worker loop finished.");
        })
    };

    // Poller Loop (Same as before)
    // ...
    let mut sigterm = signal(SignalKind::terminate())?;
    // ...
    // (Rest of the file is identical mostly, except for passing env vars)
    // I need to paste the rest of the file to ensure validity.
    
    // let mut last_activity = Instant::now();
    // let idle_timeout = Duration::from_secs(60);
    
    let mut resources = ResourceController::new();

    info!("Starting poller loop...");
    loop {
        // Resource Check
        if !resources.check_resources() {
            // Resources are exhausted. Wait a bit and skip this poll cycle.
            tokio::time::sleep(Duration::from_secs(5)).await;
            
            // Check shutdown while backoff
            if futures::poll!(Box::pin(sigterm.recv())).is_ready() {
                 info!("Received SIGTERM during throttle, stopping poller...");
                 break;
            }
            continue;
        }

        // Only poll if we have capacity in the channel
        let receive_future = sqs_client.receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(20)
            .send();

        tokio::select! {
            _ = sigterm.recv() => {
                info!("Received SIGTERM, stopping poller...");
                break;
            }
            _ = shutdown_signal.notified() => {
                info!("Shutdown signal received.");
                break;
            }
             res = receive_future => {
                match res {
                    Ok(output) => {
                        let messages = output.messages.unwrap_or_default();
                        if !messages.is_empty() {
                            // last_activity = Instant::now();
                            for msg in messages {
                                if tx.send(msg).await.is_err() {
                                    info!("Receiver dropped, stopping poller.");
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to poll SQS: {}", e);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }

    drop(tx);
    worker_handle.await?;

    info!("Exiting processor.");
    Ok(())
}
