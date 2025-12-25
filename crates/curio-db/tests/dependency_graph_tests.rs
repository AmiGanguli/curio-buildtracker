use curio_db::dependency_graph::DependencyGraph;
use std::env;
use aws_sdk_dynamodb::types::{AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType, BillingMode, GlobalSecondaryIndex, Projection, ProjectionType, ProvisionedThroughput};
use futures::StreamExt;

async fn get_client() -> DependencyGraph {
    let table_name = "test-dependency-graph";
    let region_provider = aws_config::meta::region::RegionProviderChain::default_provider().or_else(aws_sdk_dynamodb::config::Region::new("us-east-1"));
    let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region_provider).load().await;
    
    let client = if let Ok(endpoint) = env::var("DYNAMODB_ENDPOINT") {
        let conf = aws_sdk_dynamodb::config::Builder::from(&shared_config)
            .endpoint_url(endpoint)
            .build();
        aws_sdk_dynamodb::Client::from_conf(conf)
    } else {
        panic!("DYNAMODB_ENDPOINT must be set for build graph tests");
    };

    // Create Table with Schema if it doesn't exist
    let _ = client.create_table()
        .table_name(table_name)
        .attribute_definitions(AttributeDefinition::builder().attribute_name("pk").attribute_type(ScalarAttributeType::S).build().unwrap())
        .attribute_definitions(AttributeDefinition::builder().attribute_name("sk").attribute_type(ScalarAttributeType::S).build().unwrap())
        .attribute_definitions(AttributeDefinition::builder().attribute_name("gsi1pk").attribute_type(ScalarAttributeType::S).build().unwrap())
        .attribute_definitions(AttributeDefinition::builder().attribute_name("gsi1sk").attribute_type(ScalarAttributeType::S).build().unwrap())
        .key_schema(KeySchemaElement::builder().attribute_name("pk").key_type(KeyType::Hash).build().unwrap())
        .key_schema(KeySchemaElement::builder().attribute_name("sk").key_type(KeyType::Range).build().unwrap())
        // GSI1
        .global_secondary_indexes(GlobalSecondaryIndex::builder()
            .index_name("gsi1")
            .key_schema(KeySchemaElement::builder().attribute_name("gsi1pk").key_type(KeyType::Hash).build().unwrap())
            .key_schema(KeySchemaElement::builder().attribute_name("gsi1sk").key_type(KeyType::Range).build().unwrap())
            .projection(Projection::builder().projection_type(ProjectionType::All).build())
            .provisioned_throughput(ProvisionedThroughput::builder().read_capacity_units(5).write_capacity_units(5).build().unwrap())
            .build().unwrap()
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await;

    // Small delay to ensure table is ready (DynamoDB Local is fast but technically async)
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    DependencyGraph::new(client, table_name.to_string())
}

#[tokio::test]
async fn test_case_1_new_flow() {
    let client = get_client().await;
    let artifact_id = "checksum_A".to_string();
    let compute_id = "compute_1_new_flow".to_string(); // Unique ID for this test run

    client.register_artifact(artifact_id.clone(), true).await.unwrap();
    client.create_compute_node(compute_id.clone(), vec![artifact_id], "Compile".to_string()).await.unwrap();

    // Verify Dirty
    let mut dirty_stream = client.get_dirty_compute_nodes();
    let mut found = false;
    while let Some(res) = dirty_stream.next().await {
        if res.unwrap() == compute_id {
            found = true;
            break;
        }
    }
    assert!(found, "Compute node should be dirty after creation");
}

#[tokio::test]
async fn test_case_2_update_flow_queries() {
    let client = get_client().await;
    let artifact_id_a = "checksum_A_update".to_string();
    let compute_id_1 = "compute_1_update".to_string();

    client.register_artifact(artifact_id_a.clone(), true).await.unwrap();
    client.create_compute_node(compute_id_1.clone(), vec![artifact_id_a.clone()], "Compile".to_string()).await.unwrap();

    // 1. Get downstream of A -> should be compute_1
    let mut downstream = client.get_downstream_compute_nodes(artifact_id_a);
    let first = downstream.next().await.expect("stream empty").expect("error");
    assert_eq!(first, compute_id_1);

    // 2. Get details
    let (node_type, inputs) = client.get_compute_node_details(compute_id_1.clone()).await.unwrap();
    assert_eq!(node_type, "Compile");
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0], "checksum_A_update");
    
    // 3. Set Outputs (Mark Clean)
    client.set_compute_node_outputs(compute_id_1.clone(), vec!["checksum_B".to_string()]).await.unwrap();

    // 4. Verify NOT dirty
    let mut dirty_stream = client.get_dirty_compute_nodes();
    while let Some(res) = dirty_stream.next().await {
        if res.unwrap() == compute_id_1 {
            panic!("Compute node should be clean after set_compute_node_outputs");
        }
    }
}

#[tokio::test]
async fn test_gc_flow() {
    let client = get_client().await;
    let external_input = "ext_1".to_string();
    let compute_1 = "comp_1".to_string();
    let output_1 = "out_1".to_string();
    
    // 1. Setup A -> C1 -> B
    client.register_artifact(external_input.clone(), true).await.unwrap();
    // Register output too (internal)
    client.register_artifact(output_1.clone(), false).await.unwrap(); 
    
    client.create_compute_node(compute_1.clone(), vec![external_input.clone()], "Compile".to_string()).await.unwrap();
    client.set_compute_node_outputs(compute_1.clone(), vec![output_1.clone()]).await.unwrap();

    // 2. Replace C1 with C2 (which produces B2, making B orphaned)
    let compute_2 = "comp_2".to_string();
    let output_2 = "out_2".to_string();
    client.register_artifact(output_2.clone(), false).await.unwrap();
    
    // Simulate replacement:
    // Remove C1
    client.remove_compute_node(compute_1.clone()).await.unwrap();
    // Create C2
    client.create_compute_node(compute_2.clone(), vec![external_input.clone()], "Compile".to_string()).await.unwrap();
    client.set_compute_node_outputs(compute_2.clone(), vec![output_2.clone()]).await.unwrap();

    // 3. Verify Output_1 is Orphaned
    // (In a real test we'd query the GSI, but let's trust cleanup_orphans to find it)
    
    // 4. Cleanup Orphans
    // output_1 should be deleted because it has no consumers.
    let count = client.cleanup_orphans().await.unwrap();
    assert_eq!(count, 1, "Should delete 1 orphan (out_1)");
    
    // 5. Verify out_1 is gone (optional, but good)
    // could verify register_artifact fails or something, but deletion is key.
}
