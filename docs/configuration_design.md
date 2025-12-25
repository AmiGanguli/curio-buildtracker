# Curio Configuration System: Router & Reactor

This document outlines the design for the Curio configuration system, which maps external artifacts (S3 objects) to the Build Dependency Graph.

## Core Concepts

The system operates on a "Reactive" model:
1.  **Router ("Sensor")**: Matches an S3 object Key to an **Artifact Type** and extracts variables (e.g., `site_id`, `doc_id`).
2.  **Workflow ("Reactor")**: Defines a template for creating **Compute Nodes** when a specific Artifact Type is encountered.

## Configuration Schema (`curio.yaml`)

```yaml
# 1. ARTIFACT ROUTING
# Maps S3 Keys (Regex) to Artifact Types and captures named variables.
artifacts:
  - type: "document"
    match: "websites/(?P<site_id>[^/]+)/documents/(?P<doc_id>[^/]+)\\.pdf"
  
  - type: "site_config"
    match: "websites/(?P<site_id>[^/]+)/config\\.json"

  - type: "job_posting"
    match: "(?P<ats_id>[^/]+)/(?P<employer_id>[^/]+)/jobs/(?P<job_id>[^/]+)\\.json"

  - type: "ats_config"
    match: "(?P<ats_id>[^/]+)/config\\.json"

# 2. WORKFLOW RULES
# Defines the DAG construction logic triggering on artifact events.
workflows:
  # CASE 1: Document Analysis
  - trigger: "document"
    compute_node:
      type: "analyze_document"
      # Unique ID template for the Compute Node
      id: "analysis-{site_id}-{doc_id}" 
      inputs:
        - source: "self"  # The artifact that triggered this rule
        - source: "artifact"
          # Dynamic path construction using captured variables
          path: "websites/{site_id}/config.json" 
        - source: "artifact"
          # Static dependency
          path: "models/default_doc_model.bin"

  # CASE 2: Job Analysis
  - trigger: "job_posting"
    compute_node:
      type: "analyze_job"
      id: "job-analysis-{ats_id}-{employer_id}-{job_id}"
      inputs:
        - source: "self"
        - source: "artifact"
          path: "{ats_id}/{employer_id}/config.json"
        - source: "artifact"
          path: "{ats_id}/config.json"
```

## Update Logic

1.  **New Artifact Registration**:
    *   S3 Event -> Router matches `type` -> Triggers Workflow -> Creates Compute Node.
2.  **Artifact Update**:
    *   If a dependency (e.g., `config.json`) changes, the `DependencyGraph` Reverse Index (`get_downstream_compute_nodes`) identifies affected Compute Nodes.
    *   The system re-evaluates the Workflow for those nodes using the new checksum of the updated artifact.
3.  **Artifact Deletion**:
    *   Router identifies the type.
    *   System performs reverse-lookup to find Compute Nodes created *by* this artifact (where this artifact was the `self` trigger).
    *   Those Compute Nodes are removed via `remove_compute_node`.
