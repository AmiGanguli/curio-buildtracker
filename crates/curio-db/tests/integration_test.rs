use curio_db::CurioDbClient;
use std::env;
use aws_sdk_dynamodb::types::{AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType, BillingMode};

#[tokio::test]
async fn test_save_and_get_record() {
    // Ensure DYNAMODB_ENDPOINT is set (usually via env or just hardcode for this test if running locally)
    // We expect the test runner to set this, or we default if missing but only if we know we are in test mode.
    // For this test, we assume the user/script sets DYNAMODB_ENDPOINT=http://localhost:8000
    
    let table_name = "test-table-1";
    let client = CurioDbClient::new(table_name.to_string()).await;
    
    // Create the table (since it's ephemeral local DB)
    // We need to access the inner client or expose a create_table method.
    // Ideally CurioDbClient exposes what we need, or we cheat for testing by recreating the client with raw sdk.
    // Let's modify CurioDbClient to expose a create_table helper for testing? Or just use raw SDK here.
    
    let region_provider = aws_config::meta::region::RegionProviderChain::default_provider().or_else(aws_sdk_dynamodb::config::Region::new("us-east-1"));
    let shared_config = aws_config::from_env().region(region_provider).load().await;
    let raw_client = if let Ok(endpoint) = env::var("DYNAMODB_ENDPOINT") {
        let conf = aws_sdk_dynamodb::config::Builder::from(&shared_config)
            .endpoint_url(endpoint)
            .build();
        aws_sdk_dynamodb::Client::from_conf(conf)
    } else {
        panic!("DYNAMODB_ENDPOINT must be set for integration tests");
    };

    let _ = raw_client.create_table()
        .table_name(table_name)
        .attribute_definitions(AttributeDefinition::builder().attribute_name("id").attribute_type(ScalarAttributeType::S).build().expect("failed to build attr"))
        .key_schema(KeySchemaElement::builder().attribute_name("id").key_type(KeyType::Hash).build().expect("failed to build key"))
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await;
        
    // Wait a bit for table creation (instant in local usually)
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Test Save
    client.save_record("item1", "some data").await.expect("failed to save");

    // Test Get
    let result = client.get_record("item1").await.expect("failed to get");
    assert_eq!(result, Some("some data".to_string()));
}
