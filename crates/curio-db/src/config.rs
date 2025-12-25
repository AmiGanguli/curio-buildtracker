use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use regex::Regex;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CurioConfig {
    pub artifacts: Vec<ArtifactRule>,
    pub workflows: Vec<WorkflowRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRule {
    #[serde(rename = "type")]
    pub type_name: String,
    // Store the string pattern for serialization
    #[serde(rename = "match")]
    pub match_pattern: String,
    
    // Internal compiled regex, transparent to Serde
    #[serde(skip)]
    regex: Option<Regex>,
}

impl PartialEq for ArtifactRule {
    fn eq(&self, other: &Self) -> bool {
        self.type_name == other.type_name && self.match_pattern == other.match_pattern
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRule {
    pub trigger: String, // ArtifactType
    pub compute_node: ComputeNodeTemplate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ComputeNodeTemplate {
    #[serde(rename = "type")]
    pub type_name: String,
    pub id: String, // Template e.g. "analysis-{site_id}"
    pub inputs: Vec<InputTemplate>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source")]
pub enum InputTemplate {
    #[serde(rename = "self")]
    SelfArtifact,
    #[serde(rename = "artifact")]
    Artifact { path: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactMatch {
    pub type_name: String,
    pub variables: HashMap<String, String>,
}

impl CurioConfig {
    pub fn from_yaml(content: &str) -> Result<Self, serde_yaml::Error> {
        let mut config: CurioConfig = serde_yaml::from_str(content)?;
        // Compile regexes
        for rule in &mut config.artifacts {
            rule.regex = Some(Regex::new(&rule.match_pattern).expect("Invalid Regex in config"));
        }
        Ok(config)
    }

    /// Matches an S3 path against artifact rules.
    /// Returns the first match with captured variables.
    pub fn match_artifact(&self, path: &str) -> Option<ArtifactMatch> {
        for rule in &self.artifacts {
            if let Some(re) = &rule.regex {
                if let Some(caps) = re.captures(path) {
                    let mut variables = HashMap::new();
                    // extract named captures
                    for name in re.capture_names().flatten() {
                        if let Some(m) = caps.name(name) {
                            variables.insert(name.to_string(), m.as_str().to_string());
                        }
                    }
                    return Some(ArtifactMatch { 
                        type_name: rule.type_name.clone(),
                        variables
                    });
                }
            }
        }
        None
    }

    /// Finds workflows triggered by this artifact type.
    pub fn get_workflows_for_type(&self, artifact_type: &str) -> Vec<&WorkflowRule> {
        self.workflows.iter().filter(|w| w.trigger == artifact_type).collect()
    }
}
