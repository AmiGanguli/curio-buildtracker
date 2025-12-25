use curio_db::config::{CurioConfig, InputTemplate};

#[test]
fn test_config_loading_and_matching() {
    let yaml = r#"
artifacts:
  - type: "document"
    match: "websites/(?P<site_id>[^/]+)/documents/(?P<doc_id>[^/]+)\\.pdf"
  
  - type: "site_config"
    match: "websites/(?P<site_id>[^/]+)/config\\.json"

workflows:
  - trigger: "document"
    compute_node:
      type: "analyze_document"
      id: "analysis-{site_id}-{doc_id}"
      inputs:
        - source: "self"
        - source: "artifact"
          path: "websites/{site_id}/config.json"
"#;

    let config = CurioConfig::from_yaml(yaml).expect("Failed to parse YAML");
    
    // Test 1: Match Document
    let path = "websites/siteA/documents/doc1.pdf";
    let matched = config.match_artifact(path).expect("Should match document");
    assert_eq!(matched.type_name, "document");
    assert_eq!(matched.variables.get("site_id").unwrap(), "siteA");
    assert_eq!(matched.variables.get("doc_id").unwrap(), "doc1");

    // Test 2: Match Config
    let path_conf = "websites/siteB/config.json";
    let matched_conf = config.match_artifact(path_conf).expect("Should match config");
    assert_eq!(matched_conf.type_name, "site_config");
    assert_eq!(matched_conf.variables.get("site_id").unwrap(), "siteB");

    // Test 3: No Match
    assert!(config.match_artifact("random/file.txt").is_none());

    // Test 4: Get Workflows
    let workflows = config.get_workflows_for_type("document");
    assert_eq!(workflows.len(), 1);
    assert_eq!(workflows[0].compute_node.type_name, "analyze_document");
    
    // Verify template loading
    if let InputTemplate::Artifact { path } = &workflows[0].compute_node.inputs[1] {
        assert_eq!(path, "websites/{site_id}/config.json");
    } else {
        panic!("Expected Artifact input");
    }
}
