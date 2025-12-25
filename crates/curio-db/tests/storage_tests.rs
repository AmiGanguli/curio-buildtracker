use curio_db::storage::ArtifactStorage;
use aws_sdk_s3::Client;

async fn get_client_and_storage() -> (Client, ArtifactStorage) {
    let bucket = "test-bucket".to_string();
    let region_provider = aws_config::meta::region::RegionProviderChain::default_provider().or_else(aws_sdk_s3::config::Region::new("us-east-1"));
    
    // Connect to Local Moto S3
    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region_provider)
        .test_credentials() // Use dummy creds
        .load()
        .await;

    let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
        .endpoint_url("http://localhost:5000") // Moto
        .force_path_style(true) // Required for local/minio/moto
        .build();

    let client = Client::from_conf(s3_config);
    let storage = ArtifactStorage::new(client.clone(), bucket, None);
    
    (client, storage)
}

#[tokio::test]
async fn test_store_and_get_type() {
    let (client, storage) = get_client_and_storage().await;
    let _ = client.create_bucket().bucket("test-bucket").send().await; // Ensure bucket exists

    let type_name = "MyType";
    let content = "some: yaml";
    
    storage.store_compute_node_type(type_name, content).await.expect("Store failed");
    
    let retrieved = storage.get_compute_node_type(type_name).await.expect("Get failed");
    assert_eq!(retrieved, content);
}

#[tokio::test]
async fn test_save_artifact_path_logic() {
    let (client, storage) = get_client_and_storage().await;
    let _ = client.create_bucket().bucket("test-bucket").send().await;

    let id = "1234567890ABCDEF"; // 16 chars
    let metadata = "meta: data";
    let filename = "content.bin";
    let file_data = vec![1, 2, 3];

    storage.save_artifact(id, metadata, vec![(filename.to_string(), file_data.clone())]).await.expect("Save failed");

    // Retrieve via API
    let meta_retrieved = storage.get_artifact_metadata(id).await.expect("Get Valid Meta failed");
    assert_eq!(meta_retrieved, metadata);

    // Verify ACTUAL KEY in S3 matches user's new hashed path logic (6 levels)
    // Logic: 1/2/3/4/5/6/ID/
    // id = 12345678...
    // path = curio-data/artifacts/1/2/3/4/5/6/1234567890ABCDEF/artifact.yaml
    let expected_key = "curio-data/artifacts/1/2/3/4/5/6/1234567890ABCDEF/artifact.yaml";
    
    let resp = client.get_object()
        .bucket("test-bucket")
        .key(expected_key)
        .send()
        .await;
    
    assert!(resp.is_ok(), "Should find object at hashed path: {}", expected_key);
}
