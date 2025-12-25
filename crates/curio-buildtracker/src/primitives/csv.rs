use super::{Primitive, PrimitiveStatus, PrimitiveInput, PrimitiveOutput, InputDef, OutputDef, ExecutionContext};
use async_trait::async_trait;
use std::collections::HashMap;
use anyhow::{Result, anyhow};
use tokio::sync::mpsc;
use polars::prelude::*;
use polars::sql::SQLContext;

#[derive(Debug)]
pub struct CsvSelect;

#[async_trait]
impl Primitive for CsvSelect {
    fn name(&self) -> &str {
        "CsvSelect"
    }

    fn input_schema(&self) -> Vec<InputDef> {
        vec![
            InputDef {
                name: "csv".to_string(),
                description: "Input CSV".to_string(),
                mime_type: "text/csv".to_string(),
                min_count: 1,
                max_count: Some(1),
            },
            InputDef {
                name: "columns".to_string(),
                description: "Columns to select".to_string(),
                mime_type: "text/plain".to_string(),
                min_count: 1,
                max_count: None, // Multiple columns
            }
        ]
    }

    fn output_schema(&self) -> Vec<OutputDef> {
         vec![
            OutputDef {
                name: "output".to_string(),
                description: "Selected CSV".to_string(),
                mime_type: "text/csv".to_string(),
            }
        ]
    }

    async fn execute(
        &self,
        mut inputs: HashMap<String, Vec<PrimitiveInput>>,
        context: ExecutionContext<'_>,
        _status_tx: Option<mpsc::Sender<PrimitiveStatus>>,
    ) -> Result<Vec<PrimitiveOutput>> {
        let csv_path_str = match &inputs["csv"][0] {
             PrimitiveInput::ArtifactPath(p) => p.clone(),
             PrimitiveInput::Value(_) => return Err(anyhow!("CsvSelect requires artifact path input")),
        };
        // Download if needed
        // Resolve artifact path to local path using FileManager
        // Note: For CsvSelect, we might want to support S3 automatically via FileManager.
        let local_path = context.file_manager.get_file(&csv_path_str).await?;
        
        let cols: Vec<String> = inputs.remove("columns").unwrap_or_default().iter().map(|v| match v {
            PrimitiveInput::Value(s) => s.clone(),
            _ => "".to_string()
        }).collect();

        // Prepare output path
        let output_path = context.file_manager.prepare_output("csv_select_out.csv").await?;
        
        // Polars logic: Read -> Select -> Write
        let df = CsvReader::from_path(&local_path)?
            .has_header(true)
            .finish()?;

        // Select columns if specified and valid
        let result_df = if !cols.is_empty() && cols[0] != "" {
             df.select(cols)?
        } else {
             df
        };
        
        let mut file = std::fs::File::create(&output_path)?; // Polars uses std::fs::File
        CsvWriter::new(&mut file)
            .finish(&mut result_df.clone())?; // Write result
        
        // Track output (prepare_output tracks it)
        // Commit output
        let artifact_uri = context.file_manager.commit_output("csv_select_result.csv", &output_path).await?;
        
        Ok(vec![
             PrimitiveOutput {
                 name: "selected".to_string(),
                 artifact_path: artifact_uri,
             }
        ])
    }
}

#[derive(Debug)]
pub struct CsvSql;

#[async_trait]
impl Primitive for CsvSql {
    fn name(&self) -> &str {
        "CsvSql"
    }

    fn input_schema(&self) -> Vec<InputDef> {
         vec![
             InputDef { name: "query".to_string(), description: "SQL Query".to_string(), mime_type: "text/plain".to_string(), min_count: 1, max_count: Some(1) },
             // Dynamic inputs for tables?
         ]
    }

    fn output_schema(&self) -> Vec<OutputDef> {
        vec![
            OutputDef {
                name: "result".to_string(),
                description: "SQL Result CSV".to_string(),
                mime_type: "text/csv".to_string(),
            }
        ]
    }

    async fn execute(
        &self,
        mut inputs: HashMap<String, Vec<PrimitiveInput>>,
        context: ExecutionContext<'_>,
        _status_tx: Option<mpsc::Sender<PrimitiveStatus>>,
    ) -> Result<Vec<PrimitiveOutput>> {
        let query = match &inputs["query"][0] {
             PrimitiveInput::Value(s) => s.clone(),
             _ => return Err(anyhow!("Query must be inline value")),
        };
        inputs.remove("query");

        // Register remaining inputs as tables
        let mut ctx = SQLContext::new();
        // Since execute is async but Polars is mostly sync/CPU-bound, strictly we might want spawn_blocking.
        // For now, we run inline as it's a "lambda" primitive likely running in own process eventually.
        
        for (name, input_list) in inputs {
             if let PrimitiveInput::ArtifactPath(p) = &input_list[0] {
                 let local_path = context.file_manager.get_file(p).await?;
                 let lf = LazyCsvReader::new(local_path).finish()?;
                 ctx.register(&name, lf);
             }
        }
        
        let df = ctx.execute(&query)?.collect()?;
        
        let output_path = context.file_manager.prepare_output("sql_result.csv").await?;
        let mut file = std::fs::File::create(&output_path)?;
        CsvWriter::new(&mut file).finish(&mut df.clone())?;

        let artifact_uri = context.file_manager.commit_output("sql_result.csv", &output_path).await?;
        
        Ok(vec![
             PrimitiveOutput {
                name: "result".to_string(),
                artifact_path: artifact_uri,
             }
        ])
    }
}
