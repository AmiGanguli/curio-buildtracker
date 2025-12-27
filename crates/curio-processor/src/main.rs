use aws_sdk_sqs::Client;
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

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", content = "detail")]
enum EventType {
    NoOp,
    ArtifactAdded { id: String },
    ArtifactRemoved { id: String },
    ComputeNodeDirty,
    ComputeNodeObsolete,
    ArtifactObsolete,
}

#[derive(Deserialize, Debug)]
struct MessageBody {
    #[serde(rename = "detail-type")]
    detail_type: String,
    detail: serde_json::Value,
    source: String,
}

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
        // We look for any disk with high usage, or root
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

async fn handle_message(client: &Client, queue_url: &str, message: aws_sdk_sqs::types::Message) -> anyhow::Result<()> {
    if let Some(body) = &message.body {
        match serde_json::from_str::<MessageBody>(body) {
            Ok(event) => {
                if event.source != "curio.buildmanager" {
                    warn!("Ignoring message from unknown source: {}", event.source);
                    return Ok(());
                }

                 let event_enum = match event.detail_type.as_str() {
                    "NoOp" => Some(EventType::NoOp),
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
                    // Simulate work (potentially CPU intensive or Blocked)
                    tokio::time::sleep(Duration::from_millis(500)).await;
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
        client.delete_message()
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
    let client = Client::new(&config);
    let queue_url = env::var("QUEUE_URL").expect("QUEUE_URL must be set");

    let num_cores = num_cpus::get();
    let configured_concurrency = env::var("CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(num_cores * 4); // Default to 4x cores for higher throughput

    // Concurrency limit for active workers
    let concurrency = std::cmp::max(1, configured_concurrency);
    info!("Starting processor with concurrency limit: {}", concurrency);

    // Channel for messages from Poller -> Worker
    let (tx, rx) = mpsc::channel(concurrency * 2);

    let active_tasks = Arc::new(AtomicUsize::new(0));
    let shutdown_signal = Arc::new(tokio::sync::Notify::new());

    // Worker Task
    let worker_handle = {
        let client = client.clone();
        let queue_url = queue_url.clone();
        let active_tasks = active_tasks.clone();
        tokio::spawn(async move {
            ReceiverStream::new(rx)
                .for_each_concurrent(concurrency, |message| {
                    let client = client.clone();
                    let queue_url = queue_url.clone();
                    let active_tasks = active_tasks.clone();
                    async move {
                        active_tasks.fetch_add(1, Ordering::SeqCst);
                        if let Err(e) = handle_message(&client, &queue_url, message).await {
                            error!("Error processing message: {}", e);
                        }
                        active_tasks.fetch_sub(1, Ordering::SeqCst);
                    }
                })
                .await;
            info!("Worker loop finished.");
        })
    };

    // Poller Loop
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut last_activity = Instant::now();
    let idle_timeout = Duration::from_secs(60);
    
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
        // mpsc::Sender::capacity() isn't directly available to check "count" easily without blocking send,
        // but we want to avoid over-fetching if channel is full.
        // `max_number_of_messages(10)` puts up to 10 items.
        // If channel is full, `tx.send().await` will block, effectively throttling the poller naturally.
        // So we rely on backpressure from the channel + resource check.

        let receive_future = client.receive_message()
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
                            last_activity = Instant::now();
                            for msg in messages {
                                if tx.send(msg).await.is_err() {
                                    info!("Receiver dropped, stopping poller.");
                                    return Ok(());
                                }
                            }
                        } else {
                            // Empty poll
                            // let now = Instant::now();
                            // if now.duration_since(last_activity) > idle_timeout {
                            //    if active_tasks.load(Ordering::SeqCst) == 0 {
                            //        info!("Idle timeout detected (1 minute inactive & 0 active tasks). Exiting.");
                            //        break;
                            //    }
                            // }
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
