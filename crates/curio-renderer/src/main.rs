use lambda_runtime::{service_fn, LambdaEvent, Error};
use serde_json::{json, Value};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_dynamodb::Client as DynamoClient;
use std::collections::HashMap;
use std::env;
use anyhow::{Context, Result};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .with_ansi(false)
        .without_time()
        .init();

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = S3Client::new(&config);
    let dynamo_client = DynamoClient::new(&config);

    let func = service_fn(move |event| {
        let s3 = s3_client.clone();
        let dynamo = dynamo_client.clone();
        async move { handler(event, s3, dynamo).await }
    });

    lambda_runtime::run(func).await?;
    Ok(())
}

async fn handler(event: LambdaEvent<Value>, s3: S3Client, dynamo: DynamoClient) -> Result<Value, Error> {
    let payload = event.payload;
    println!("Received event: {}", payload);

    let query_params = payload.get("queryStringParameters")
        .and_then(|qp| qp.as_object())
        .cloned()
        .unwrap_or_default();

    let id = match query_params.get("id").and_then(|v| v.as_str()) {
        Some(id) => id.to_string(),
        None => return Ok(api_response(400, json!({"error": "Missing 'id' parameter"}))),
    };

    let format = query_params.get("format")
        .and_then(|v| v.as_str())
        .unwrap_or("text");

    // 1. Resolve Bucket from Config
    let config_str = env::var("EXTERNAL_INPUTS_CONFIG").unwrap_or_else(|_| "[]".to_string());
    let config_json: Vec<Value> = serde_json::from_str(&config_str).unwrap_or_default();
    
    let mut target_bucket = None;
    if id.starts_with("external/") {
         let parts: Vec<&str> = id.split('/').collect();
         if parts.len() > 1 {
             let input_name = parts[1];
             for cfg in config_json {
                 if cfg["name"] == input_name {
                     target_bucket = cfg["bucket"].as_str().map(|s| s.to_string());
                     break;
                 }
             }
         }
    }

    let bucket = match target_bucket {
        Some(b) => b,
        None => return Ok(api_response(404, json!({"error": "Bucket not found for item"}))),
    };

    // 2. Resolve S3 Key from DynamoDB
    let table_name = env::var("TABLE_NAME").unwrap_or_default();
    let s3_key = if !table_name.is_empty() {
        match dynamo.get_item()
            .table_name(&table_name)
            .key("id", aws_sdk_dynamodb::types::AttributeValue::S(id.clone()))
            .send().await 
        {
            Ok(resp) => {
                 if let Some(item) = resp.item {
                     item.get("s3_key").and_then(|k| k.as_s().ok()).cloned().unwrap_or(id.clone())
                 } else {
                     return Ok(api_response(404, json!({"error": "Item not found in catalog"})));
                 }
            },
            Err(e) => {
                println!("DynamoDB error: {}", e);
                return Ok(api_response(500, json!({"error": "Database lookup failed"})));
            }
        }
    } else {
        // Fallback or dev mode without table?
        id.clone()
    };

    // 3. Fetch from S3
    let content_str = match s3.get_object()
        .bucket(&bucket)
        .key(&s3_key)
        .send().await 
    {
        Ok(output) => {
            let bytes = output.body.collect().await?.into_bytes();
            String::from_utf8_lossy(&bytes).to_string()
        },
        Err(e) => {
             println!("S3 fetch error: {}", e);
             return Ok(api_response(500, json!({"error": format!("S3 fetch failed: {}", e)})));
        }
    };

    // 3a. Extract Field if pointer requested
    let pointer = query_params.get("pointer").and_then(|v| v.as_str());
    let content_to_render = if let Some(ptr) = pointer {
        // Parse JSON
        let json_val: Value = match serde_json::from_str(&content_str) {
            Ok(v) => v,
            Err(e) => return Ok(api_response(400, json!({"error": format!("Failed to parse S3 content as JSON: {}", e)}))),
        };

        match json_val.pointer(ptr) {
            Some(v) => {
                match v {
                    Value::String(s) => s.clone(),
                    _ => v.to_string(), // Fallback for non-string
                }
            },
            None => return Ok(api_response(404, json!({"error": format!("Pointer '{}' not found in document", ptr)}))),
        }
    } else {
        content_str
    };

    // 4. Render
    let result = if format == "text" {
        // Decode HTML entities (e.g. &lt; -> <) before parsing tags
        let decoded = html_escape::decode_html_entities(&content_to_render);
        // Use html2text to strip tags and format
        html2text::from_read(decoded.as_bytes(), 80)
    } else {
        content_to_render
    };

    Ok(api_response(200, json!({ "content": result })))
}

fn api_response(status: u16, body: Value) -> Value {
    let headers = json!({
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
        "Access-Control-Allow-Methods": "OPTIONS,GET"
    });

    json!({
        "statusCode": status,
        "headers": headers,
        "body": body.to_string()
    })
}
