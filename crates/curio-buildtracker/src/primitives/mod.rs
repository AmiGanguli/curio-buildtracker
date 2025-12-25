#![allow(dead_code, unused_imports, unused_variables)]
use async_trait::async_trait;
use std::collections::HashMap;
use anyhow::Result;
use tokio::sync::mpsc;
use std::fmt::Debug;

pub mod io;
pub mod transform;
pub mod aggregate;
pub mod csv;

use crate::file_manager::FileManager;

pub use io::{FetchUrl, S3Get, S3Put};
pub use transform::{JsonSelect, TemplateRender};
pub use aggregate::{MergeJson, Concatenate};
pub use csv::{CsvSelect, CsvSql};

mod tests;

/// Context passed to primitive execution.
pub struct ExecutionContext<'a> {
    pub file_manager: &'a dyn FileManager,
}


/// Represents a definition of an input expected by the primitive.
#[derive(Debug, Clone)]
pub struct InputDef {
    pub name: String,
    pub description: String,
    pub mime_type: String,   // E.g. "application/json" or "text/*"
    pub min_count: usize,    // 0 = optional, 1 = required, >1 = array
    pub max_count: Option<usize>, // None = unlimited
}

/// Represents a definition of an output produced by the primitive.
#[derive(Debug, Clone)]
pub struct OutputDef {
    pub name: String,
    pub description: String,
    pub mime_type: String,
}

/// Represents the actual input data passed to execution.
#[derive(Debug, Clone)]
pub enum PrimitiveInput {
    ArtifactPath(String),
    Value(String),
}

/// Represents the output produced.
#[derive(Debug, Clone)]
pub struct PrimitiveOutput {
    pub name: String,
    pub artifact_path: String,
}

/// Status updates sent during execution.
#[derive(Debug, Clone)]
pub enum PrimitiveStatus {
    Starting,
    Progress(f32, String),
    Completed,
    Failed(String),
}

#[async_trait]
pub trait Primitive: Send + Sync + Debug {
    /// Unique name of the primitive (e.g. "FetchUrl")
    fn name(&self) -> &str;

    /// Schema Introspection
    fn input_schema(&self) -> Vec<InputDef>;
    fn output_schema(&self) -> Vec<OutputDef>;

    /// Execution logic.
    /// * `inputs`: Map of argument name -> List of inputs.
    /// * `context`: Execution environment (artifacts, etc).
    async fn execute(
        &self,
        inputs: HashMap<String, Vec<PrimitiveInput>>,
        context: ExecutionContext<'_>,
        status_tx: Option<mpsc::Sender<PrimitiveStatus>>,
    ) -> Result<Vec<PrimitiveOutput>>;
}
