use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;

pub struct ArtifactStorage {
    client: Client,
    bucket: String,
    prefix: String,
}

impl ArtifactStorage {
    pub fn new(client: Client, bucket: String, prefix: Option<String>) -> Self {
        Self {
            client,
            bucket,
            prefix: prefix.unwrap_or_else(|| "curio-data".to_string()),
        }
    }

    /// Helper to construct the hashed path for an artifact.
    /// Input: "1234567890ABCDEF"
    /// Output: "{prefix}/artifacts/1/2/3/4/5/6/1234567890ABCDEF/"
    fn get_artifact_path(&self, checksum: &str) -> String {
        if checksum.len() < 6 {
            // Fallback or error? For now simple fallback to root of artifacts
            return format!("{}/artifacts/{}", self.prefix, checksum);
        }
        let c = checksum.chars().collect::<Vec<_>>();
        format!("{}/artifacts/{}/{}/{}/{}/{}/{}/{}/", 
            self.prefix,
            c[0], c[1], c[2], c[3], c[4], c[5],
            checksum
        )
    }
    
    fn get_type_path(&self, type_name: &str) -> String {
        format!("{}/compute_node_types/{}.yaml", self.prefix, type_name)
    }

    /// Stores a compute node type definition (YAML).
    pub async fn store_compute_node_type(&self, type_name: &str, content: &str) -> Result<(), aws_sdk_s3::Error> {
        let key = self.get_type_path(type_name);
        
        self.client.put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(content.as_bytes().to_vec()))
            .send()
            .await?;
        
        Ok(())
    }

    /// Retrieves a compute node type definition.
    pub async fn get_compute_node_type(&self, type_name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let key = self.get_type_path(type_name);
        let resp = self.client.get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        
        let data = resp.body.collect().await?;
        Ok(String::from_utf8(data.into_bytes().to_vec())?)
    }

    /// Saves an artifact's metadata and content files.
    /// `files` is a list of (filename, content_bytes).
    pub async fn save_artifact(&self, id: &str, metadata_yaml: &str, files: Vec<(String, Vec<u8>)>) -> Result<(), aws_sdk_s3::Error> {
        let base_path = self.get_artifact_path(id);
        
        // 1. Save artifact.yaml
        let meta_key = format!("{}artifact.yaml", base_path);
        self.client.put_object()
            .bucket(&self.bucket)
            .key(meta_key)
            .body(ByteStream::from(metadata_yaml.as_bytes().to_vec()))
            .send()
            .await?;
        
        // 2. Save content files
        for (name, data) in files {
            let file_key = format!("{}{}", base_path, name);
            self.client.put_object()
                .bucket(&self.bucket)
                .key(file_key)
                .body(ByteStream::from(data))
                .send()
                .await?;
        }

        Ok(())
    }

    /// Retrieves artifact.yaml metadata.
    pub async fn get_artifact_metadata(&self, id: &str) -> Result<String, Box<dyn std::error::Error>> {
        let base_path = self.get_artifact_path(id);
        let key = format!("{}artifact.yaml", base_path);
        
        let resp = self.client.get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
            
        let data = resp.body.collect().await?;
        Ok(String::from_utf8(data.into_bytes().to_vec())?)
    }
}
