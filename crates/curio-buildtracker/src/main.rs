mod primitives;
pub mod file_manager;

use lambda_runtime::{service_fn, Error, LambdaEvent};
use aws_lambda_events::event::sqs::SqsEvent;
use serde::{Deserialize, Serialize};
use curio_db::CurioDbClient;
use std::env;

// Embed the config.json file at compile time
const CONFIG_JSON: &str = include_str!("../config.json");

#[derive(Deserialize, Serialize)]
struct Config {
    message: String,
    database_url: String,
}

#[derive(Serialize)]
struct Response {
    msg: String,
}

async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<Response, Error> {
    // Parse the embedded config
    let config: Config = serde_json::from_str(CONFIG_JSON)?;
    
    // Initialize DynamoDB Client
    let table_name = env::var("TABLE_NAME").expect("TABLE_NAME must be set");
    let db_client = CurioDbClient::new(table_name).await;
    
    // Process records
    for record in event.payload.records {
        if let Some(body) = record.body {
            tracing::info!("Received message: {}", body);
            tracing::info!("Config Message: {}", config.message);
            
            // Save to DynamoDB
            // Use messageId as ID, body as data
            if let Some(id) = record.message_id {
                let _ = db_client.save_record(&id, &body).await;
                tracing::info!("Saved record {} to DynamoDB", id);
            }
        }
    }

    Ok(Response {
        msg: "Processed SQS messages".to_string(),
    })
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .without_time()
        .init();

    let func = service_fn(function_handler);
    lambda_runtime::run(func).await?;
    Ok(())
}
