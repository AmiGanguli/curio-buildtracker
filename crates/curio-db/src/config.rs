use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use regex::Regex;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CurioConfig {
    pub artifacts: Vec<ArtifactRule>,
    #[serde(default)]
    pub external_inputs: Vec<ExternalInputRule>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalInputRule {
    pub name: String,
    pub bucket: String,
    pub prefix: Option<String>,
    #[serde(rename = "match")]
    pub match_pattern: String,
    
    // Derived fields from capture groups
    #[serde(skip)]
    pub fields: Vec<String>,

    // Internal compiled regex
    #[serde(skip)]
    pub regex: Option<Regex>,
}

// ... (PartialEq impl for ArtifactRule remains same if needed, or we rely on default)
// But since I'm replacing the whole block including struct definition, I should keep it.
// Actually, I can just replace the struct and add the new one.

impl PartialEq for ArtifactRule {
    fn eq(&self, other: &Self) -> bool {
        self.type_name == other.type_name && self.match_pattern == other.match_pattern
    }
}

// Implement PartialEq for ExternalInputRule manually to ignore regex
impl PartialEq for ExternalInputRule {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && 
        self.bucket == other.bucket && 
        self.prefix == other.prefix &&
        self.match_pattern == other.match_pattern
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
    #[serde(rename = "external")]
    External { name: String },
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
        for rule in &mut config.external_inputs {
             // Extract field names from regex
             let regex = Regex::new(&rule.match_pattern).expect("Invalid Regex in external input config");
             let mut fields = Vec::new();
             for name in regex.capture_names().flatten() {
                 fields.push(name.to_string());
             }
             rule.fields = fields; // We need to add this field to ExternalInputRule
             rule.regex = Some(regex);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_external_input_regex() {
        let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
        // curio-db is in crates/curio-db, so we go up two levels to root, then examples/config
        let config_path = PathBuf::from(manifest_dir)
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("examples/config/curio.yaml");

        let yaml = fs::read_to_string(&config_path)
            .expect(&format!("Failed to read config file at {:?}", config_path));

        let config = CurioConfig::from_yaml(&yaml).expect("Failed to parse YAML");
        let rule = &config.external_inputs[0];
        let re = rule.regex.as_ref().expect("Regex not compiled");

        let path = "greenhouse/tokens/1848ventures/jobs_by_id/5539249004.json";
        let caps = re.captures(path).expect("Regex did not match path");

        assert_eq!(&caps["ats"], "greenhouse");
        assert_eq!(&caps["token"], "1848ventures");
        assert_eq!(&caps["job_id"], "5539249004");
    }
}
