use async_trait::async_trait;
use std::path::{Path, PathBuf};
use anyhow::{Result, anyhow, Context};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use std::fmt::Debug;
use aws_sdk_s3::Client;
// use aws_sdk_s3::primitives::ByteStream; 
use std::sync::Mutex;
use uuid::Uuid;

#[async_trait]
pub trait FileManager: Send + Sync + Debug {
    /// Request an input file. Returns a local path that is guaranteed to exist.
    /// Handles downloading if necessary.
    async fn get_file(&self, uri: &str) -> Result<PathBuf>;

    /// Request a local writable path for an output artifact.
    /// The caller should write to this path.
    /// If the URI is not fully specified (e.g. just a filename), the manager decides placement.
    async fn prepare_output(&self, uri: &str) -> Result<PathBuf>;

    /// Commit a prepared output.
    /// This signals that writing is complete and the file should be persisted/uploaded to its final URI.
    /// Returns the stable artifact URI.
    async fn commit_output(&self, uri: &str, temp_path: &Path) -> Result<String>;
    
    /// Cleans up tracked local files.
    async fn cleanup(&self) -> Result<()>;
}

#[derive(Debug)]
pub struct LocalFileManager {
    pub base_dir: PathBuf,
    temp_files: Mutex<Vec<PathBuf>>,
}

impl LocalFileManager {
    pub fn new(base_dir: PathBuf) -> Self {
        Self { 
            base_dir,
            temp_files: Mutex::new(Vec::new()),
        }
    }

    fn track(&self, path: PathBuf) {
        let mut piles = self.temp_files.lock().unwrap();
        piles.push(path);
    }
    fn drain_tracked(&self) -> Vec<PathBuf> {
        let mut piles = self.temp_files.lock().unwrap();
        std::mem::take(&mut *piles)
    }
}

async fn delete_files(files: Vec<PathBuf>) {
    for path in files {
        if path.exists() {
            let _ = fs::remove_file(path).await;
        }
    }
}

impl Drop for LocalFileManager {
    fn drop(&mut self) {
        let files = self.drain_tracked();
        if !files.is_empty() {
            tokio::spawn(delete_files(files));
        }
    }
}

#[async_trait]
impl FileManager for LocalFileManager {
    async fn get_file(&self, uri: &str) -> Result<PathBuf> {
        let path_str = uri.strip_prefix("file://").unwrap_or(uri);
        let path = PathBuf::from(path_str);
        
        if path.exists() {
            return Ok(path);
        }

        // If relative, check base_dir
        let joined = self.base_dir.join(path_str);
        if joined.exists() {
             return Ok(joined);
        }
        
        if uri.starts_with("s3://") {
             return Err(anyhow!("LocalFileManager cannot handle S3 URI: {}", uri));
        }

        // If not found, error? Or return path if it's expected to exist?
        // get_file expects EXISTENCE.
        Err(anyhow!("File not found: {}", uri))
    }

    async fn prepare_output(&self, _uri: &str) -> Result<PathBuf> {
        // Create a temp file in base_dir/temp/uuid
        let temp_dir = self.base_dir.join("temp");
        fs::create_dir_all(&temp_dir).await?;
        let filename = Uuid::new_v4().to_string();
        let temp_path = temp_dir.join(filename);
        
        self.track(temp_path.clone());
        Ok(temp_path)
    }

    async fn commit_output(&self, _uri: &str, temp_path: &Path) -> Result<String> {
        // Local strategy: keep the file where it is? Or move it to stable path?
        // If we want stable URI, we should move it to storage. 
        // But `prepare_output` just gave us a temp path.
        // Let's assume `commit_output` moves it to a "permanent" location if `uri` implies one.
        // Actually, `uri` might be the desired output name.
        
        // Simpler for Local: Move to base_dir/{hash or name}
        // Let's assume `uri` is a key hint.
        // We'll calculate a hash or use the hint.
        
        // If uri is empty/generic, generate one.
        let key = if _uri.is_empty() { Uuid::new_v4().to_string() } else { _uri.to_string() };
        let dest = self.base_dir.join(&key);
        
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).await?;
        }
        
        fs::copy(temp_path, &dest).await?;
        // Note: we don't delete temp_path here, cleanup() will do it.
        // Or we could move (rename) it? 
        // If we rename, track() list contains invalid path.
        // Copy is safer for "Cleanup tracks temp files".
        
        // Return file URI
        let abs = dest.canonicalize().unwrap_or(dest);
        Ok(format!("file://{}", abs.to_string_lossy()))
    }

    async fn cleanup(&self) -> Result<()> {
        let files = self.drain_tracked();
        delete_files(files).await;
        Ok(())
    }
}

#[derive(Debug)]
pub struct S3FileManager {
    client: Client,
    pub bucket: String,
    pub cache_dir: PathBuf,
    temp_files: Mutex<Vec<PathBuf>>,
}

impl S3FileManager {
    pub fn new(client: Client, bucket: String) -> Self {
        Self {
            client,
            bucket,
            cache_dir: std::env::temp_dir().join("curio_s3_cache"),
            temp_files: Mutex::new(Vec::new()),
        }
    }

    fn track(&self, path: PathBuf) {
        let mut piles = self.temp_files.lock().unwrap();
        piles.push(path);
    }
    fn drain_tracked(&self) -> Vec<PathBuf> {
        let mut piles = self.temp_files.lock().unwrap();
        std::mem::take(&mut *piles)
    }
}

impl Drop for S3FileManager {
    fn drop(&mut self) {
        let files = self.drain_tracked();
        if !files.is_empty() {
            tokio::spawn(delete_files(files));
        }
    }
}

#[async_trait]
impl FileManager for S3FileManager {
    async fn get_file(&self, uri: &str) -> Result<PathBuf> {
        if uri.starts_with("file://") {
             return Ok(PathBuf::from(uri.strip_prefix("file://").unwrap()));
        }

        // Parse S3 URI
        if !uri.starts_with("s3://") {
             return Err(anyhow!("Invalid S3 URI: {}", uri));
        }
        
        let without_scheme = &uri[5..];
        let parts: Vec<&str> = without_scheme.splitn(2, '/').collect();
        if parts.len() != 2 {
             return Err(anyhow!("Invalid S3 URI format: {}", uri));
        }
        let bucket = parts[0];
        let key = parts[1];
        
        // Check cache
        let hash = md5::compute(uri);
        let filename = format!("{:x}", hash);
        let ext = Path::new(key).extension().map(|e| e.to_string_lossy().to_string()).unwrap_or_default();
        let final_name = if !ext.is_empty() { format!("{}.{}", filename, ext) } else { filename };

        let dest_path = self.cache_dir.join(&bucket).join(final_name);

        self.track(dest_path.clone());

        if dest_path.exists() {
             return Ok(dest_path);
        }

        if let Some(parent) = dest_path.parent() {
             fs::create_dir_all(parent).await?;
        }

        let mut resp = self.client.get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .context(format!("Failed to get object {} from {}", key, bucket))?;
        
        let mut file = fs::File::create(&dest_path).await?;
        
        while let Some(bytes) = resp.body.try_next().await? {
             file.write_all(&bytes).await?;
        }

        Ok(dest_path)
    }

    async fn prepare_output(&self, uri: &str) -> Result<PathBuf> {
         // Create local temp file
         let temp_dir = std::env::temp_dir().join("curio_s3_temp");
         fs::create_dir_all(&temp_dir).await?;
         
         // Use uri as a hint for name/ext?
         let filename = if uri.is_empty() {
             Uuid::new_v4().to_string()
         } else {
             uri.replace("/", "_") // rudimentary sanitization
         };
         
         let temp_path = temp_dir.join(filename);
         self.track(temp_path.clone());
         
         Ok(temp_path)
    }

    async fn commit_output(&self, uri: &str, temp_path: &Path) -> Result<String> {
        // Upload to S3.
        // uri is expected to be a KEY suffix or full s3 path?
        // Let's assume it's a relative KEY for our bucket unless it starts with s3://
        
        let (bucket, key) = if uri.starts_with("s3://") {
             let without_scheme = &uri[5..];
             let parts: Vec<&str> = without_scheme.splitn(2, '/').collect();
             if parts.len() != 2 {
                  return Err(anyhow!("Invalid S3 URI format: {}", uri));
             }
             (parts[0].to_string(), parts[1].to_string())
        } else {
             (self.bucket.clone(), uri.to_string())
        };
        
        let clean_key = key.trim_start_matches('/');
        
        let body = aws_sdk_s3::primitives::ByteStream::from_path(temp_path).await?;
        
        self.client.put_object()
            .bucket(&bucket)
            .key(clean_key)
            .body(body)
            .send()
            .await
            .context(format!("Failed to put object {} to {}", clean_key, bucket))?;
            
        Ok(format!("s3://{}/{}", bucket, clean_key))
    }

    async fn cleanup(&self) -> Result<()> {
        let files = self.drain_tracked();
        delete_files(files).await;
        Ok(())
    }
}

