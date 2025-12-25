# Compute Primitives

This document defines the standard "primitive" Compute Node types available in the Curio pipeline. These are built-in operations designed to handle common data science and engineering tasks without requiring custom container logic.

## 1. I/O & Ingestion
*Getting data into and out of the system.*

| Primitive | Description | Inputs | Outputs |
|---|---|---|---|
| **`FetchUrl`** | Downloads a file from a public URL. | `url` (string), `headers` (optional map) | Content artifact (auto-detected type) |
| **`S3Get`** | Downloads a specific object from an external S3 bucket. | `bucket`, `key`, `region` | Content artifact |
| **`S3Put`** | Uploads an artifact to an external S3 bucket. | `artifact` (source), `bucket`, `key` | Receipt/Status |

## 2. Transformation
*Changing the shape, format, or content of data.*

| Primitive | Description | Inputs | Outputs |
|---|---|---|---|
| **`ExtractText`** | Converts documents (PDF, Docx, HTML) to plain text. | `document` | Text artifact (`.txt`) |
| **`JsonSelect`** | Extracts a subset of a JSON object using a query (e.g., JMESPath). | `json`, `query` | JSON artifact |
| **`TemplateRender`** | Renders a template string/file using input variables. | `template`, `context` (JSON) | Rendered artifact |

## 3. Aggregation & Control
*Combining multiple inputs.*

| Primitive | Description | Inputs | Outputs |
|---|---|---|---|
| **`MergeJson`** | Deep-merges multiple JSON files in order. | `inputs` (list of JSON artifacts) | Merged JSON artifact |
| **`Concatenate`** | Appends multiple text/binary files into one. | `inputs` (list) | Single artifact |

## 4. Tabular Data (CSV/DataFrames)
*Relational operations on structured data. Implementation backed by high-performance engines (e.g., Polars).*

| Primitive | Description | Inputs | Outputs |
|---|---|---|---|
| **`CsvSelect`** | Keeps only specified columns. | `csv`, `columns` (list of strings) | CSV artifact |
| **`CsvFilter`** | Filters rows based on a condition or expression. | `csv`, `condition` (string expr) | CSV artifact |
| **`CsvSort`** | Sorts rows by a column. | `csv`, `by` (col name), `desc` (bool) | Sorted CSV |
| **`CsvJoin`** | SQL-style join of two CSVs. | `left`, `right`, `on` (col), `how` (inner/left/outer) | Joined CSV |
| **`CsvStack`** | Vertically concatenates (unions) multiple CSVs with same schema. | `inputs` (list of CSVs) | Stacked CSV |
| **`CsvGroupAgg`** | Groups by column(s) and computes aggregates. | `csv`, `group_by`, `aggs` (map of col->op) | Summary CSV |
| **`CsvDedupe`** | Removes duplicate rows. | `csv`, `subset` (optional cols) | Deduped CSV |
| **`CsvSql`** | Executes a SQL query against CSV inputs. | `query` (SQL string), `tables` (map: name->csv) | Result CSV |

## 5. Execution (Generic)
*Running arbitrary logic.*

| Primitive | Description | Inputs | Outputs |
|---|---|---|---|
| **`ShellCommand`** | Runs a bash script. (Low reproducibility). | `script`, `env` | Stdout/File capture |
| **`ContainerRun`** | Executes a Docker container. | `image`, `command`, `mounts` | Output directory capture |
