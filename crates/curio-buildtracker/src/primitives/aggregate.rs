use super::{Primitive, InputDef, OutputDef, PrimitiveInput, PrimitiveOutput, PrimitiveStatus, ExecutionContext};
use async_trait::async_trait;
use std::collections::HashMap;
use anyhow::{Result, anyhow};
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct MergeJson;

#[async_trait]
impl Primitive for MergeJson {
    fn name(&self) -> &str {
        "MergeJson"
    }
    
    fn input_schema(&self) -> Vec<InputDef> {
        vec![
            InputDef {
                name: "inputs".to_string(),
                description: "List of JSON files to merge".to_string(),
                mime_type: "application/json".to_string(),
                min_count: 1,
                max_count: None, // Unlimited
            }
        ]
    }

    fn output_schema(&self) -> Vec<OutputDef> {
        vec![
             OutputDef {
                name: "merged".to_string(),
                description: "Merged JSON".to_string(),
                mime_type: "application/json".to_string(),
            }
        ]
    }

    async fn execute(
        &self,
        mut inputs: HashMap<String, Vec<PrimitiveInput>>,
        context: ExecutionContext<'_>,
        _status_tx: Option<mpsc::Sender<PrimitiveStatus>>,
    ) -> Result<Vec<PrimitiveOutput>> {
        let json_list = inputs.remove("inputs").ok_or_else(|| anyhow!("Missing inputs"))?;
        let mut merged = serde_json::Value::Object(serde_json::Map::new());

        for input in json_list {
            let content = match input {
                PrimitiveInput::Value(s) => s,
                PrimitiveInput::ArtifactPath(p) => {
                    let local = context.file_manager.get_file(&p).await?;
                    tokio::fs::read_to_string(local).await?
                },
            };
            let v: serde_json::Value = serde_json::from_str(&content).unwrap_or(serde_json::json!({}));
            merge(&mut merged, v);
        }

        let result_str = serde_json::to_string(&merged)?;
        let temp_path = context.file_manager.prepare_output("merged.json").await?;
        tokio::fs::write(&temp_path, result_str).await?;
        let artifact_uri = context.file_manager.commit_output("merged_result.json", &temp_path).await?;

        Ok(vec![
            PrimitiveOutput {
                name: "merged".to_string(),
                artifact_path: artifact_uri,
            }
        ])
    }
}

fn merge(a: &mut serde_json::Value, b: serde_json::Value) {
    match (a, b) {
        (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
            for (alt_key, v) in b {
                merge(a.entry(alt_key).or_insert(serde_json::Value::Null), v);
            }
        }
        (a, b) => *a = b,
    }
}

#[derive(Debug)]
pub struct Concatenate;

#[async_trait]
impl Primitive for Concatenate {
    fn name(&self) -> &str {
        "Concatenate"
    }

    fn input_schema(&self) -> Vec<InputDef> {
        vec![
            InputDef {
                name: "inputs".to_string(),
                description: "Files to split/concat".to_string(),
                mime_type: "*/*".to_string(),
                min_count: 1,
                max_count: None,
            }
        ]
    }

    fn output_schema(&self) -> Vec<OutputDef> {
         vec![
            OutputDef {
                name: "output".to_string(),
                description: "Concatenated file".to_string(),
                mime_type: "*/*".to_string(),
            }
        ]
    }

    async fn execute(
        &self,
        mut inputs: HashMap<String, Vec<PrimitiveInput>>,
        context: ExecutionContext<'_>,
        _status_tx: Option<mpsc::Sender<PrimitiveStatus>>,
    ) -> Result<Vec<PrimitiveOutput>> {
        let input_list = inputs.remove("inputs").ok_or_else(|| anyhow!("Missing inputs"))?;
        
        let temp_path = context.file_manager.prepare_output("concat.bin").await?;
        let mut out_file = tokio::fs::File::create(&temp_path).await?;
        use tokio::io::AsyncWriteExt; // ensure write_all is available
        use tokio::io::AsyncReadExt;

        for input in input_list {
             let path_str = match input {
                 PrimitiveInput::ArtifactPath(p) => p,
                 PrimitiveInput::Value(_) => return Err(anyhow!("Concatenate expects artifact paths")),
             };
             let local = context.file_manager.get_file(&path_str).await?;
             let mut f = tokio::fs::File::open(local).await?;
             let mut buf = Vec::new();
             f.read_to_end(&mut buf).await?;
             out_file.write_all(&buf).await?;
        }
        
        // Commit
        let artifact_uri = context.file_manager.commit_output("concatenated_result.bin", &temp_path).await?;

        Ok(vec![
            PrimitiveOutput {
                name: "output".to_string(),
                artifact_path: artifact_uri,
            }
        ])
    }
}
