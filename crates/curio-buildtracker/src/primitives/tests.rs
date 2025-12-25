#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::{Primitive, PrimitiveInput, ExecutionContext};
    use crate::primitives::io::FetchUrl;
    use crate::primitives::transform::{JsonSelect, TemplateRender};
    use crate::primitives::csv::CsvSelect;
    use crate::file_manager::{FileManager, LocalFileManager};
    use std::collections::HashMap;
    use tokio::sync::mpsc;
    use anyhow::Result;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_fetch_url() -> Result<()> {
        let p = FetchUrl;
        let mut inputs = HashMap::new();
        inputs.insert("url".to_string(), vec![PrimitiveInput::Value("http://example.com".to_string())]);

        let (tx, mut rx) = mpsc::channel(10);
        tokio::spawn(async move {
            while let Some(status) = rx.recv().await {
                println!("Status: {:?}", status);
            }
        });

        let mgr = LocalFileManager::new(PathBuf::from("/tmp"));
        let ctx = ExecutionContext { file_manager: &mgr };

        let outputs = p.execute(inputs, ctx, Some(tx)).await?;
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].name, "content");
        // LocalFileManager returns file:// URIs
        assert!(outputs[0].artifact_path.starts_with("file://"));
        Ok(())
    }

    #[tokio::test]
    async fn test_json_select() -> Result<()> {
        let p = JsonSelect;
        let mut inputs = HashMap::new();
        let json_doc = r#"{"people": [{"name": "Alice"}, {"name": "Bob"}]}"#;
        inputs.insert("json".to_string(), vec![PrimitiveInput::Value(json_doc.to_string())]);
        inputs.insert("query".to_string(), vec![PrimitiveInput::Value("people[*].name".to_string())]);
        
        let mgr = LocalFileManager::new(PathBuf::from("/tmp"));
        let ctx = ExecutionContext { file_manager: &mgr };

        let outputs = p.execute(inputs, ctx, None).await?;
        assert_eq!(outputs.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_template_render() -> Result<()> {
        let p = TemplateRender;
        let mut inputs = HashMap::new();
        inputs.insert("template".to_string(), vec![PrimitiveInput::Value("Hello {{ name }}".to_string())]);
        inputs.insert("context".to_string(), vec![PrimitiveInput::Value(r#"{"name": "World"}"#.to_string())]);

        let mgr = LocalFileManager::new(PathBuf::from("/tmp"));
        let ctx = ExecutionContext { file_manager: &mgr };

        let outputs = p.execute(inputs, ctx, None).await?;
        assert_eq!(outputs.len(), 1);
        assert!(outputs[0].artifact_path.starts_with("file://") || outputs[0].artifact_path.contains("rendered"));
        Ok(())
    }

    #[tokio::test]

    async fn test_csv_select() -> Result<()> {
        let p = CsvSelect;
        let mut inputs = HashMap::new();
        let test_file = PathBuf::from("/tmp/test_select.csv");
        tokio::fs::write(&test_file, "col1,col2\nval1,val2").await?;
        
        inputs.insert("csv".to_string(), vec![PrimitiveInput::ArtifactPath("/tmp/test_select.csv".to_string())]);
        inputs.insert("columns".to_string(), vec![PrimitiveInput::Value("col1".to_string())]);

        let mgr = LocalFileManager::new(PathBuf::from("/tmp"));
        let ctx = ExecutionContext { file_manager: &mgr };

        let outputs = p.execute(inputs, ctx, None).await?;
        assert_eq!(outputs.len(), 1);
        
        // Check content
        let out_uri = &outputs[0].artifact_path;
        let out_path = PathBuf::from(out_uri.strip_prefix("file://").unwrap());
        let content = tokio::fs::read_to_string(out_path).await?;
        // Polars CSV writer output might vary (quoting etc), but checks for val1 and col1
        assert!(content.contains("col1"));
        assert!(content.contains("val1"));
        assert!(!content.contains("col2")); // Should be filtered out
        Ok(())
    }

    use crate::primitives::csv::CsvSql;
    #[tokio::test]
    async fn test_csv_sql() -> Result<()> {
        let p = CsvSql;
        let mut inputs = HashMap::new();
        // Create table t1
        let t1 = PathBuf::from("/tmp/t1.csv");
        tokio::fs::write(&t1, "id,val\n1,a\n2,b").await?;
        
        // Inputs: query, and tables
        inputs.insert("query".to_string(), vec![PrimitiveInput::Value("SELECT * FROM t1 WHERE id = 1".to_string())]);
        inputs.insert("t1".to_string(), vec![PrimitiveInput::ArtifactPath("/tmp/t1.csv".to_string())]);
        
        let mgr = LocalFileManager::new(PathBuf::from("/tmp"));
        let ctx = ExecutionContext { file_manager: &mgr };
        
        let outputs = p.execute(inputs, ctx, None).await?;
        assert_eq!(outputs.len(), 1);
        
        let out_uri = &outputs[0].artifact_path;
        let out_path = PathBuf::from(out_uri.strip_prefix("file://").unwrap());
        let content = tokio::fs::read_to_string(out_path).await?;
        assert!(content.contains("1,a"));
        assert!(!content.contains("2,b"));
        
        Ok(())
    }
    
    use crate::primitives::aggregate::Concatenate;
    #[tokio::test]
    async fn test_concatenate() -> Result<()> {
        let p = Concatenate;
        let mut inputs = HashMap::new();
        
        let f1 = PathBuf::from("/tmp/c1.txt"); tokio::fs::write(&f1, "Hello ").await?;
        let f2 = PathBuf::from("/tmp/c2.txt"); tokio::fs::write(&f2, "World").await?;
        
        inputs.insert("inputs".to_string(), vec![
            PrimitiveInput::ArtifactPath("/tmp/c1.txt".to_string()),
            PrimitiveInput::ArtifactPath("/tmp/c2.txt".to_string()),
        ]);
        
        let mgr = LocalFileManager::new(PathBuf::from("/tmp"));
        let ctx = ExecutionContext { file_manager: &mgr };
        
        let outputs = p.execute(inputs, ctx, None).await?;
        
        let out_uri = &outputs[0].artifact_path;
        let out_path = PathBuf::from(out_uri.strip_prefix("file://").unwrap());
        let content = tokio::fs::read_to_string(out_path).await?;
        assert_eq!(content, "Hello World");
        
        Ok(())
    }


    #[tokio::test]
    async fn test_cleanup() -> Result<()> {
        let temp_dir = std::env::temp_dir().join("curio_test_cleanup");
        tokio::fs::create_dir_all(&temp_dir).await?;
        let mgr = LocalFileManager::new(temp_dir.clone());

        // Use prepare_output to create a tracked file
        let tracked_file = mgr.prepare_output("to_delete.txt").await?;
        tokio::fs::write(&tracked_file, "delete me").await?;

        assert!(tracked_file.exists());
        mgr.cleanup().await?;
        assert!(!tracked_file.exists());
        
        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cleanup() -> Result<()> {
         let temp_dir = std::env::temp_dir().join("curio_test_drop");
         tokio::fs::create_dir_all(&temp_dir).await?;
         let tracked_file = {
             let mgr = LocalFileManager::new(temp_dir.clone());
             let p = mgr.prepare_output("drop_me.txt").await?;
             tokio::fs::write(&p, "I should die").await?;
             p
             // mgr drops here
         };

         // Give background task time to run
         tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
         
         assert!(!tracked_file.exists(), "File should have been deleted by Drop");
         Ok(())
    }
}
