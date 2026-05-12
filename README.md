# JSON Doc Generator

A Python web application that parses ETL pipeline configuration JSON files and generates human-readable Markdown documentation with data lineage and optimization recommendations.

## Features

- **Upload & Parse** — Upload one or more pipeline JSON files via a web interface
- **Full Lineage** — Traces data flow from source tables through transformations to output targets
- **Multi-Source Support** — Handles JDBC (Oracle, MySQL), Hive, and Parquet sources
- **Compressed Mode** — Automatically summarizes large pipelines (70+ steps, 80-100 columns) into readable flow tables
- **Optimization Recommendations** — Detects duplicate joins, pass-through maps, and consolidation opportunities
- **Cross-Pipeline Dependencies** — Identifies when one pipeline's output feeds another's input
- **Variable Resolution** — Maps `${variable}` references to their usage across queries and expressions
- **Execution Sequencing** — Respects properties files for pipeline ordering within domain folders

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the app
python -m src.app
```

Open http://localhost:5000 in your browser, upload JSON files, and get documentation.

## Project Structure

```
JsonDocgenerator/
├── README.md
├── requirements.txt
├── json_to_unified_csv.py          # Reference: JSON→CSV converter (for understanding schema)
│
├── src/                             # Application source code
│   ├── __init__.py
│   ├── app.py                       # Flask web app (upload, preview, download)
│   ├── models.py                    # Dataclasses and enums (PipelineModel, LineageDAG)
│   ├── parser.py                    # JSON parser for all source/transform/output types
│   ├── validator.py                 # File extension and schema validation
│   ├── pretty_printer.py           # Round-trip serialization back to Pipeline_JSON
│   ├── lineage.py                   # DAG construction and topological sort
│   ├── variable_resolver.py        # ${variable} reference detection
│   ├── properties_reader.py        # Execution sequence from properties files
│   ├── analyzer.py                  # Optimization detection and recommendations
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
        └── json-doc-generator/
            ├── .config.kiro
            ├── requirements.md
            ├── design.md
            └── tasks.md
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

## Execution Sequencing

Create a `sequence.properties` file in each domain folder to define execution order:

```properties
# finance/sequence.properties
gl_reconciliation_complex.json
ap_aging_complex.json
```

If no properties file exists, files are ordered alphabetically.

## Optimization Recommendations

The analyzer detects these patterns:

- **Duplicate join conditions** — Same join reused across multiple dataframes
- **Duplicate column mappings** — Identical column patterns that could be merged
- **Pass-through maps** — Maps that only forward columns with no filter (inline candidates)
- **Same-pair joins** — Multiple joins between the same two sources
- **Chained maps** — Sequential maps without filters that can be collapsed
- **Unused sources** — Defined but never referenced source dataframes

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Upload interface |
| POST | `/upload` | Upload JSON files and generate docs |
| GET | `/preview` | Rendered HTML preview of documentation |
| GET | `/download` | Download generated Markdown file |

## Tech Stack

- Python 3.11+
- Flask (web framework)
- Jinja2 (Markdown templating)
- dataclasses (internal models)
- markdown (HTML preview rendering)
