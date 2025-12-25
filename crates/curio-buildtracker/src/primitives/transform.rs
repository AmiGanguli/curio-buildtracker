use super::{Primitive, InputDef, OutputDef, PrimitiveInput, PrimitiveOutput, PrimitiveStatus, ExecutionContext};
use async_trait::async_trait;
use std::collections::HashMap;
use anyhow::{Result, anyhow};
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct JsonSelect;

#[async_trait]
impl Primitive for JsonSelect {
    fn name(&self) -> &str {
        "JsonSelect"
    }

    fn input_schema(&self) -> Vec<InputDef> {
        vec![
            InputDef {
                name: "json".to_string(),
                description: "The JSON document".to_string(),
                mime_type: "application/json".to_string(),
                min_count: 1,
                max_count: Some(1),
            },
            InputDef {
                name: "query".to_string(),
                description: "JMESPath query string".to_string(),
                mime_type: "text/plain".to_string(),
                min_count: 1,
                max_count: Some(1),
            }
        ]
    }

    fn output_schema(&self) -> Vec<OutputDef> {
        vec![
            OutputDef {
                name: "result".to_string(),
                description: "The selected JSON fragment".to_string(),
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
        // Extract query
        let query_inputs = inputs.remove("query").ok_or_else(|| anyhow!("Missing query"))?;
        let query_str = match &query_inputs[0] {
            PrimitiveInput::Value(s) => s.clone(),
            _ => return Err(anyhow!("Query must be inline value")),
        };

        // Extract JSON
        let json_inputs = inputs.remove("json").ok_or_else(|| anyhow!("Missing json"))?;
        let json_content = match &json_inputs[0] {
            PrimitiveInput::Value(s) => s.clone(),
            PrimitiveInput::ArtifactPath(p) => {
                let local = context.file_manager.get_file(p).await?;
                tokio::fs::read_to_string(local).await?
            }
        };

        let data = serde_json::from_str(&json_content).unwrap_or(serde_json::json!({}));
        let result_str = {
            let expr = jmespath::compile(&query_str)?;
            let result = expr.search(&data)?;
            serde_json::to_string(&result)?
        };
        
        // Save result (result_str is String, which is Send)
        let temp_path = context.file_manager.prepare_output("json_select.json").await?;
        tokio::fs::write(&temp_path, result_str).await?;
        
        let artifact_uri = context.file_manager.commit_output("json_select_result.json", &temp_path).await?;
        
        Ok(vec![
            PrimitiveOutput {
                name: "result".to_string(),
                artifact_path: artifact_uri, 
            }
        ])
    }
}

#[derive(Debug)]
pub struct TemplateRender;

#[async_trait]
impl Primitive for TemplateRender {
    fn name(&self) -> &str {
        "TemplateRender"
    }

    fn input_schema(&self) -> Vec<InputDef> {
        vec![
            InputDef {
                name: "template".to_string(),
                description: "Template string (Tera syntax)".to_string(),
                mime_type: "text/plain".to_string(),
                min_count: 1,
                max_count: Some(1),
            },
            InputDef {
                name: "context".to_string(),
                description: "JSON Context".to_string(),
                mime_type: "application/json".to_string(),
                min_count: 1,
                max_count: Some(1),
            }
        ]
    }

    fn output_schema(&self) -> Vec<OutputDef> {
        vec![
            OutputDef {
                name: "rendered".to_string(),
                description: "Resulting text".to_string(),
                mime_type: "text/plain".to_string(),
            }
        ]
    }

    async fn execute(
        &self,
        mut inputs: HashMap<String, Vec<PrimitiveInput>>,
        context: ExecutionContext<'_>,
        _status_tx: Option<mpsc::Sender<PrimitiveStatus>>,
    ) -> Result<Vec<PrimitiveOutput>> {
        let template_input = inputs.remove("template").ok_or_else(|| anyhow!("Missing template"))?;
        let template_str = match &template_input[0] {
            PrimitiveInput::Value(s) => s.clone(),
            PrimitiveInput::ArtifactPath(p) => {
                let local = context.file_manager.get_file(p).await?;
                tokio::fs::read_to_string(local).await?
            }
        };

        let context_input = inputs.remove("context").ok_or_else(|| anyhow!("Missing context"))?;
        let context_str = match &context_input[0] {
            PrimitiveInput::Value(s) => s.clone(),
            PrimitiveInput::ArtifactPath(p) => {
                let local = context.file_manager.get_file(p).await?;
                tokio::fs::read_to_string(local).await?
            }
        };
        let context_val: serde_json::Value = serde_json::from_str(&context_str).unwrap_or(serde_json::json!({}));
        let ctx = tera::Context::from_value(context_val)?;

        let rendered = tera::Tera::one_off(&template_str, &ctx, false)?;
        
        let temp_path = context.file_manager.prepare_output("rendered.txt").await?;
        tokio::fs::write(&temp_path, rendered).await?;
        let artifact_uri = context.file_manager.commit_output("rendered_result.txt", &temp_path).await?;
        
        Ok(vec![
            PrimitiveOutput {
                name: "rendered".to_string(),
                artifact_path: artifact_uri,
            }
        ])
    }
}
