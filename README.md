# JSON Doc Generator & Pipeline Optimizer

A Python web application that parses ETL pipeline configuration JSON files, generates documentation with data lineage, optimizes pipelines, and produces Spark execution DAGs for Scala code generation.

## Features

- **Upload & Parse** — Upload one or more pipeline JSON files via a web interface
- **Full Lineage** — Traces data flow from source tables through transformations to output targets
- **Multi-Source Support** — Handles JDBC (Oracle, MySQL), Hive, and Parquet sources
- **Compressed Mode** — Automatically summarizes large pipelines (70+ steps, 80-100 columns) into readable flow tables
- **Optimization Recommendations** — Detects duplicate joins, pass-through maps, and consolidation opportunities
- **Pipeline Optimizer** — Automatically rewrites JSONs based on recommendations (inline, collapse, consolidate, merge)
- **Cross-Pipeline Optimization** — Detects when one pipeline's output feeds another's input and collapses intermediate writes
- **Equivalence Proof** — Automated reasoning proving optimized pipeline produces same results as original
- **Spark DAG Generator** — Produces typed execution DAG with operations and traversal logic for Spark Scala code generation
- **File Sequencing UI** — Sortable file list (▲/▼) to define execution order for cross-pipeline flow
- **Variable Resolution** — Maps `${variable}` references to their usage across queries and expressions
- **Mermaid Diagrams** — System flow and per-pipeline lineage visualizations

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the app
python -m src.app
```

Open http://localhost:5001 in your browser.

## Pages

| URL | Purpose |
|-----|---------|
| `/` | Documentation generator — upload JSONs, set sequence, get Markdown docs |
| `/optimize` | Pipeline optimizer — rewrite JSONs, get equivalence proof |
| `/spark-dag` | Spark DAG generator — get typed execution DAG for Scala code gen |

All pages support file sequencing via a sortable list UI.

## Project Structure

```
JsonDocgenerator/
├── README.md
├── requirements.txt
├── push.sh                          # Git push helper (uses .pat token)
├── json_to_unified_csv.py           # Reference: JSON→CSV converter (schema understanding)
│
├── src/                             # Application source code
│   ├── __init__.py
│   ├── app.py                       # Flask web app (all endpoints + UI)
│   ├── models.py                    # Core dataclasses (PipelineModel, LineageDAG)
│   ├── parser.py                    # JSON parser (all source/transform/output types)
│   ├── validator.py                 # File extension and schema validation
│   ├── pretty_printer.py            # Round-trip serialization back to Pipeline_JSON
│   ├── lineage.py                   # DAG construction and topological sort
│   ├── variable_resolver.py         # ${variable} reference detection
│   ├── properties_reader.py         # Execution sequence from properties files
│   ├── analyzer.py                  # Optimization detection and recommendations
│   ├── optimizer.py                 # Pipeline optimizer (5 rules + cross-pipeline collapse)
│   ├── doc_generator.py             # Markdown documentation generator
│   ├── spark_dag_models.py          # Spark DAG typed nodes (8 operation types)
│   ├── spark_dag_generator.py       # PipelineModel → SparkDAG transformation
│   ├── spark_dag_traversal.py       # Topological, reverse, level-based traversal
│   ├── spark_dag_serializer.py      # JSON serialize/deserialize for Scala consumer
│   ├── spark_dag_renderer.py        # Mermaid rendering + Markdown report
│   └── templates/
│       ├── pipeline.md.j2           # Full per-pipeline Markdown template
│       ├── pipeline_compressed.md.j2 # Compressed template for large pipelines
│       └── consolidated.md.j2       # Multi-file consolidated template
│
├── jsons/                           # Sample pipeline JSON files
│   ├── finance/
│   │   ├── ap_aging_complex.json
│   │   └── gl_reconciliation_complex.json
│   ├── hr/
│   │   ├── attrition_complex.json
│   │   └── headcount_complex.json
│   ├── marketing/
│   │   └── campaign_roi_complex.json
│   ├── sales/
│   │   ├── commission_complex.json
│   │   └── forecast_complex.json
│   └── supply_chain/
│       └── inventory_complex.json
│
├── tests/                           # Test suite
│   └── __init__.py
│
└── .kiro/                           # Spec documents
    └── specs/
        ├── json-doc-generator/      # Documentation generator spec
        ├── pipeline-optimizer/      # Optimizer spec
        └── spark-dag-generator/     # Spark DAG spec
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Doc generator upload form with sequence UI |
| POST | `/upload` | Upload JSONs + sequence → generate documentation |
| GET | `/preview` | Rendered HTML preview of documentation |
| GET | `/download` | Download generated Markdown file |
| GET/POST | `/optimize` | Pipeline optimizer with sequence UI |
| GET | `/optimize/preview` | Optimization report + equivalence proof + mermaid |
| GET | `/optimize/download` | Download zip (optimized JSONs + report + proof + diagrams) |
| GET/POST | `/spark-dag` | Spark DAG generator with sequence UI + mode selector |
| GET | `/spark-dag/download/json` | Download SparkDAG as JSON (for Scala code gen) |
| GET | `/spark-dag/download/markdown` | Download Markdown report with mermaid + traversal |

## Pipeline Optimizer

The optimizer applies 5 single-pipeline rules in safe order:
1. **Remove unused sources** — eliminate dead configuration
2. **Inline pass-through maps** — remove intermediate steps that only forward columns
3. **Collapse chained maps** — merge sequential maps without filters
4. **Consolidate joins** — combine multiple joins between same source pair
5. **Merge duplicate maps** — deduplicate identical column patterns

Plus **cross-pipeline collapse** when sequence is provided — eliminates intermediate write-then-read between sequential pipelines.

Each optimization produces:
- Optimized JSON (drop-in replacement)
- Optimization Report (before/after metrics)
- Equivalence Proof (step-by-step reasoning why output is identical)

## Spark DAG Generator

Generates a typed intermediate representation (IR) for Spark Scala code generation:

- **8 node types**: SparkRead, SparkSelect, SparkFilter, SparkJoin, SparkUnion, SparkGroupByAgg, SparkSort, SparkWrite
- **3 traversal modes**: Topological (execution order), Reverse (lineage tracing), Level-based (parallelism)
- **3 DAG modes**: Optimized (default), Original (before optimization), Both (side-by-side)
- **File sequencing**: Sortable UI to define cross-pipeline execution order
- **JSON output**: Complete metadata per node for a Scala code generator to consume
- **Mermaid visualization**: Distinct shapes per operation type

### DAG JSON Schema (for Scala consumer)

```json
{
  "pipeline_name": "AP_AGING_REPORT_JOB",
  "nodes": [
    {"id": "read:oracle_invoices_df", "operation": "SparkRead", "source_df_name": "oracle_invoices_df", "metadata": {...}},
    {"id": "filter:invoice_base_df", "operation": "SparkFilter", "metadata": {"conditions": [...]}},
    {"id": "select:invoice_base_df", "operation": "SparkSelect", "metadata": {"columns": [...]}},
    {"id": "join:ap_with_vendor_df", "operation": "SparkJoin", "metadata": {"join_type": "inner", ...}},
    {"id": "agg:ap_aging_summary_df", "operation": "SparkGroupByAgg", "metadata": {"group_by_columns": [...], "aggregations": [...]}},
    {"id": "sort:ap_aging_summary_df", "operation": "SparkSort", "metadata": {"columns": [...]}},
    {"id": "write:4002001", "operation": "SparkWrite", "metadata": {"format": "jdbc", "table_name": "ap_aging_report", ...}}
  ],
  "edges": [{"from_node_id": "...", "to_node_id": "...", "edge_type": "feeds|joins_left|joins_right"}],
  "execution_order": ["read:...", "filter:...", "select:...", ...],
  "levels": {"0": ["read:..."], "1": ["filter:..."], ...}
}
```

## Pipeline JSON Schema

Each JSON file represents an ETL pipeline with these sections:

| Section | Purpose |
|---------|---------|
| `variablesMP` | Job name and runtime parameters |
| `connectionDetailsMP` | Database connections (Oracle, MySQL, Hive) |
| `srcDFsOptionsMP` | Source queries, dbtable refs, or parquet paths |
| `sourceDfPipeline` | Source dataframe definitions |
| `derivedDfPipeline` | Transformation steps (map, join, union, agg) |
| `derivedDfPipelineMappingMP` | Column mappings, filters, join conditions, aggregations |
| `derivedDfPipelineMapSrcMap` | Source-to-derived mappings |
| `derivedDfPipelineJoiSrcMap` | Join source mappings (type, sourceA, sourceB) |
| `outputDfPipeline` | Output target definitions |
| `tgtDFsMP` | Target format and table names |
| `tgtDFsOptionsMP` | Write options (batchsize, mode, path) |

## File Sequencing

All three pages (docs, optimizer, spark-dag) support file sequencing:
1. Upload multiple JSON files
2. A sortable list appears with ▲/▼ buttons
3. Reorder files top-to-bottom to define execution sequence
4. The system uses this order for cross-pipeline dependency detection and optimization

## Tech Stack

- Python 3.12+
- Flask (web framework)
- Jinja2 (Markdown templating)
- dataclasses (internal models)
- markdown (HTML preview rendering)
