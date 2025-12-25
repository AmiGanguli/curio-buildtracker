use super::{Primitive, InputDef, OutputDef, PrimitiveInput, PrimitiveOutput, PrimitiveStatus, ExecutionContext};
use async_trait::async_trait;
use std::collections::HashMap;
use anyhow::{Result, anyhow};
use tokio::sync::mpsc;
use md5;
use tokio::io::AsyncWriteExt; // For file writing
// use aws_config;
// use aws_sdk_s3;

#[derive(Debug)]
pub struct FetchUrl;

#[async_trait]
impl Primitive for FetchUrl {
    fn name(&self) -> &str {
        "FetchUrl"
    }

    fn input_schema(&self) -> Vec<InputDef> {
        vec![
            InputDef {
                name: "url".to_string(),
                description: "The URL to fetch".to_string(),
                mime_type: "text/plain".to_string(),
                min_count: 1,
                max_count: Some(1),
            }
        ]
    }

    fn output_schema(&self) -> Vec<OutputDef> {
        vec![
            OutputDef {
                name: "content".to_string(),
                description: "The fetched content body".to_string(),
                mime_type: "*/*".to_string(), // Could be anything
            }
        ]
    }

    async fn execute(
        &self,
        mut inputs: HashMap<String, Vec<PrimitiveInput>>,
        context: ExecutionContext<'_>,
        status_tx: Option<mpsc::Sender<PrimitiveStatus>>,
    ) -> Result<Vec<PrimitiveOutput>> {
        if let Some(tx) = &status_tx {
            let _ = tx.send(PrimitiveStatus::Starting).await;
        }

        // Handle single input "url"
        let url_inputs = inputs.remove("url").ok_or_else(|| anyhow!("Missing required input 'url'"))?;
        if url_inputs.is_empty() {
             return Err(anyhow!("Missing required input 'url'"));
        }
        let url_input = &url_inputs[0]; 

        let url = match url_input {
            PrimitiveInput::Value(s) => s.clone(),
            PrimitiveInput::ArtifactPath(_) => return Err(anyhow!("FetchUrl expects inline value for url, not artifact path")),
        };

        if let Some(tx) = &status_tx {
            let _ = tx.send(PrimitiveStatus::Progress(0.1, format!("Fetching {}", url))).await;
        }
        
        // Prepare output
        let temp_path = context.file_manager.prepare_output("fetch_url_temp").await?;

        // Real implementation using reqwest
        let resp = reqwest::get(&url).await?;
        let bytes = resp.bytes().await?;
        
        tokio::fs::write(&temp_path, &bytes).await?;
        
        // Determine predictable filename or hash for artifact URI if needed, 
        // or just let file_manager decide. 
        // We used MD5 of URL before.
        let filename = format!("fetch_{}", md5::compute(&url).iter().map(|b| format!("{:02x}", b)).collect::<String>());

        // Commit artifact
        let artifact_uri = context.file_manager.commit_output(&filename, &temp_path).await?; // Use filename as hint

        if let Some(tx) = &status_tx {
            let _ = tx.send(PrimitiveStatus::Completed).await;
        }

        Ok(vec![
            PrimitiveOutput {
                name: "content".to_string(),
                artifact_path: artifact_uri,
            }
        ])
    }
}

#[derive(Debug)]
pub struct S3Get;

#[async_trait]
impl Primitive for S3Get {
    fn name(&self) -> &str {
        "S3Get"
    }

    fn input_schema(&self) -> Vec<InputDef> {
        vec![
            InputDef { name: "bucket".to_string(), description: "Bucket name".to_string(), mime_type: "text/plain".to_string(), min_count: 1, max_count: Some(1) },
            InputDef { name: "key".to_string(), description: "Object key".to_string(), mime_type: "text/plain".to_string(), min_count: 1, max_count: Some(1) },
        ]
    }

    fn output_schema(&self) -> Vec<OutputDef> {
        vec![
            OutputDef { name: "file".to_string(), description: "Downloaded file".to_string(), mime_type: "*/*".to_string() }
        ]
    }

    async fn execute(
        &self,
        inputs: HashMap<String, Vec<PrimitiveInput>>,
        context: ExecutionContext<'_>,
        _status_tx: Option<mpsc::Sender<PrimitiveStatus>>,
    ) -> Result<Vec<PrimitiveOutput>> {
        let bucket = match &inputs["bucket"][0] { PrimitiveInput::Value(s) => s, _ => return Err(anyhow!("Invalid input")) };
        let key = match &inputs["key"][0] { PrimitiveInput::Value(s) => s, _ => return Err(anyhow!("Invalid input")) };

        // We want to IMPORT from external S3 to our system.
        // We can use get_file on "s3://bucket/key" if FileManager supports generic S3.
        // S3FileManager supports "s3://", but it assumes it can read it? Yes, uses standard client.
        // If LocalFileManager, it explicitly errors on s3://.
        // If we are running locally, we can't easily S3Get unless we have creds/net.
        // But for S3Get, we probably want to download it and then COMMIT it as an internal artifact?
        // Or if S3FileManager, maybe we just reference it?
        
        // Let's stick to "Download to temp, Commit to storage".
        // This ensures the artifact is "ingested".
        
        let _s3_uri = format!("s3://{}/{}", bucket, key);
        
        // Use a temporary FileManager? Or is `context.file_manager` capable?
        // context.file_manager might be Local or S3.
        // If Local, it can't `get_file("s3://...")`.
        // So `S3Get` primitive implies we can access S3.
        // We might need an ephemeral S3 client here if FileManager is Local?
        // OR we say `S3Get` requires an environment capable of S3.
        
        // Let's assume for now we use a fresh AWS client to download, then file_manager to commit.
        // This makes `S3Get` robust even with LocalFileManager (as long as we have creds).
        
        // ... But wait, if we are in Lambda (S3FileManager), `get_file` works on S3.
        // So we can try `context.file_manager.get_file(&s3_uri)`?
        // But LocalFileManager errors.
        
        // Let's just do manual download (Ingest) -> Commit.
        
        let config = aws_config::load_from_env().await;
        let client = aws_sdk_s3::Client::new(&config);
        
        let temp_path = context.file_manager.prepare_output("s3_import_temp").await?;
        
        let mut resp = client.get_object().bucket(bucket).key(key).send().await?;
        let mut file = tokio::fs::File::create(&temp_path).await?;
        while let Some(bytes) = resp.body.try_next().await? {
             file.write_all(&bytes).await?;
        }
        
        // Determine internal key
        let internal_key = format!("imported/{}/{}", bucket, key);
        
        let artifact_uri = context.file_manager.commit_output(&internal_key, &temp_path).await?;

        Ok(vec![
            PrimitiveOutput {
                name: "file".to_string(),
                artifact_path: artifact_uri, 
            }
        ])
    }
}

#[derive(Debug)]
pub struct S3Put;

#[async_trait]
impl Primitive for S3Put {
    fn name(&self) -> &str {
        "S3Put"
    }

    fn input_schema(&self) -> Vec<InputDef> {
        vec![
            InputDef { name: "file".to_string(), description: "File to upload".to_string(), mime_type: "*/*".to_string(), min_count: 1, max_count: Some(1) },
            InputDef { name: "bucket".to_string(), description: "Dest Bucket".to_string(), mime_type: "text/plain".to_string(), min_count: 1, max_count: Some(1) },
            InputDef { name: "key".to_string(), description: "Dest Key".to_string(), mime_type: "text/plain".to_string(), min_count: 1, max_count: Some(1) },
        ]
    }

    fn output_schema(&self) -> Vec<OutputDef> {
         vec![]
    }

    async fn execute(
        &self,
        mut inputs: HashMap<String, Vec<PrimitiveInput>>,
        context: ExecutionContext<'_>,
        _status_tx: Option<mpsc::Sender<PrimitiveStatus>>,
    ) -> Result<Vec<PrimitiveOutput>> {
         let file_input = inputs.remove("file").ok_or_else(|| anyhow!("Missing file input"))?;
         let file_path_str = match &file_input[0] {
             PrimitiveInput::ArtifactPath(p) => p,
             PrimitiveInput::Value(_) => return Err(anyhow!("S3Put expects artifact path for file")),
         };
         
         let bucket = match &inputs["bucket"][0] { PrimitiveInput::Value(s) => s, _ => return Err(anyhow!("Invalid input bucket")) };
         let key = match &inputs["key"][0] { PrimitiveInput::Value(s) => s, _ => return Err(anyhow!("Invalid input key")) };

         // Get local path of the artifact to upload
         let local_path = context.file_manager.get_file(file_path_str).await?;
         
         // Export to external S3
         let config = aws_config::load_from_env().await;
         let client = aws_sdk_s3::Client::new(&config);
         
         let body = aws_sdk_s3::primitives::ByteStream::from_path(&local_path).await?;
         
         client.put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .send()
            .await?;

        Ok(vec![])
    }
}
