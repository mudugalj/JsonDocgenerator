# Implementation Plan: JSON Doc Generator

## Overview

Implement a Python Flask web application that parses ETL pipeline configuration JSON files and generates human-readable Markdown documentation. The implementation follows the module structure defined in the design: validator, parser, pretty_printer, lineage, doc_generator, properties_reader, variable_resolver, and app. Property-based tests use Hypothesis; unit tests use pytest.

## Tasks

- [x] 1. Project setup and data models
  - [x] 1.1 Create project structure and install dependencies
    - Create directory structure: `src/`, `src/templates/`, `tests/`
    - Create `requirements.txt` with Flask, Jinja2, hypothesis, pytest, markdown dependencies
    - Create `__init__.py` files for packages
    - _Requirements: 12.1_

  - [x] 1.2 Implement core data models
    - Create `src/models.py` with all dataclass definitions from the design
    - Implement enums: SourceType, TransformationType, JoinType, DatabaseType, OutputFormat
    - Implement dataclasses: Variable, JobInfo, Connection, ColumnMapping, SourceDataframe, DerivedDataframe, OutputTarget, PipelineModel
    - Implement lineage dataclasses: LineageNode, LineageEdge, LineageDAG, CrossPipelineLink
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.9, 2.10, 2.11_

- [x] 2. Validator module
  - [x] 2.1 Implement file extension and schema validation
    - Create `src/validator.py` with `validate_extension(filename)` and `validate_schema(data)` functions
    - Extension validation: accept only `.json` (case-insensitive)
    - Schema validation: check top-level keys, required sections (sourceDfPipeline, derivedDfPipeline, outputDfPipeline), required fields per entry
    - Return descriptive error messages identifying filename and missing/malformed sections
    - _Requirements: 1.2, 1.3, 1.4_

  - [ ]* 2.2 Write property tests for validator
    - **Property 2: File Extension Validation**
    - **Property 3: Schema Validation Error Identification**
    - **Validates: Requirements 1.2, 1.3, 1.4**

- [x] 3. Parser module
  - [x] 3.1 Implement variables and connections parsing
    - Create `src/parser.py` with `parse_pipeline(data)` function
    - Extract variablesMP: job ID and key-value option pairs
    - Extract connectionDetailsMP: numeric ID, driver, url, user, password; derive DatabaseType from driver string
    - _Requirements: 2.1, 2.2_

  - [x] 3.2 Implement source dataframe parsing
    - Parse sourceDfPipeline entries: ID, type (jdbc/hive/parquet), connection reference, source reference
    - Parse srcDFsOptionsMP: resolve numeric ID prefix to query, dbtable, path, or sourceFilter
    - Handle all source types: JDBC queries, Hive queries, Parquet file paths with sourceFilter
    - _Requirements: 2.3, 2.4_

  - [x] 3.3 Implement derived dataframe parsing
    - Parse derivedDfPipeline entries: ID and type (map/join/union/agg)
    - Parse derivedDfPipelineMappingMP: column mappings, expressions, filters, join expressions, group-by, aggregations, sort orders, srcAFilter, srcBFilter
    - Parse derivedDfPipelineMapSrcMap: source-to-derived mappings
    - Parse derivedDfPipelineJoiSrcMap: join source mappings (joinType, sourceA, sourceB)
    - Handle union type: extract sourceA/sourceB column mappings from `.sourceA`/`.sourceB` suffixed IDs
    - Handle multi-condition joins from multiple Options array entries
    - Handle joinExpressionOR entries separately from AND conditions
    - _Requirements: 2.5, 2.6, 2.7, 2.8, 2.12, 2.13, 2.15_

  - [x] 3.4 Implement expression parsing (col, expr, LITERAL patterns)
    - Implement regex-based parsing for `col(source.column).alias(name)`, `expr(...).alias(name)`, `LITERAL(value)`, `col(column).desc()`
    - Parse column mappings into ColumnMapping dataclass instances
    - Recognize and extract LITERAL(value) syntax as constant value assignments
    - _Requirements: 2.6, 2.14_

  - [x] 3.5 Implement output dataframe parsing
    - Parse outputDfPipeline: target and connection references, handle same dataframe ID appearing multiple times
    - Parse tgtDFsMP: target format and table name (parquet targets may lack TABLE entry)
    - Parse tgtDFsOptionsMP: batchsize, mode, path values
    - _Requirements: 2.9, 2.10, 2.11_

  - [ ]* 3.6 Write property test for parse/print round-trip
    - **Property 1: Parse/Print Round-Trip**
    - **Validates: Requirements 2.1–2.15, 3.1, 3.2**

- [x] 4. Pretty printer module
  - [x] 4.1 Implement pretty printer serialization
    - Create `src/pretty_printer.py` with `print_pipeline(model)` function
    - Serialize PipelineModel back to valid Pipeline_JSON dict conforming to original schema
    - Reconstruct all sections: variablesMP, connectionDetailsMP, srcDFsOptionsMP, sourceDfPipeline, derivedDfPipeline, derivedDfPipelineMappingMP, derivedDfPipelineMapSrcMap, derivedDfPipelineJoiSrcMap, outputDfPipeline, tgtDFsMP, tgtDFsOptionsMP
    - _Requirements: 3.1, 3.2_

- [x] 5. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Lineage resolver module
  - [x] 6.1 Implement DAG construction and topological sort
    - Create `src/lineage.py` with `resolve_lineage(model)` function
    - Build DAG: nodes are dataframe IDs (source/derived/output), edges represent data flow
    - Implement topological sort for dependency ordering
    - Implement `trace_to_sources(output_id)` to find all paths from output back to source nodes
    - Detect circular dependencies and raise validation errors
    - _Requirements: 8.1, 8.2_

  - [x] 6.2 Implement cross-pipeline lineage resolution
    - Implement `resolve_cross_pipeline_lineage(models)` function
    - Match output targets of one pipeline to source tables of another
    - Return CrossPipelineLink instances identifying source/target pipelines and matching tables
    - _Requirements: 8.3_

  - [ ]* 6.3 Write property tests for lineage
    - **Property 10: Dependency Order**
    - **Property 11: Lineage Completeness**
    - **Validates: Requirements 7.10, 8.1, 8.2**

- [x] 7. Variable resolver module
  - [x] 7.1 Implement variable reference detection and cross-reference
    - Create `src/variable_resolver.py` with `find_variable_references(model)` and `find_unresolved_variables(model)` functions
    - Scan all source queries (srcDFsOptionsMP query values) for `${variable_name}` patterns
    - Scan all derived dataframe expressions (derivedDfPipelineMappingMP) for `${variable_name}` patterns
    - Build cross-reference map: variable → list of source/derived dataframes that use it
    - Identify unresolved variables (referenced but not defined in variablesMP)
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5_

  - [ ]* 7.2 Write property tests for variable resolver
    - **Property 17: Variable Cross-Reference Completeness**
    - **Property 18: Unresolved Variable Warning**
    - **Validates: Requirements 13.1–13.5**

- [x] 8. Properties file reader module
  - [x] 8.1 Implement properties file reading and execution sequencing
    - Create `src/properties_reader.py` with `read_properties(domain_folder)` function
    - Read properties file from domain folder to determine execution sequence
    - Return ordered list of filenames
    - Handle missing properties file: return empty list (caller handles fallback)
    - _Requirements: 10.1, 10.2, 10.3, 10.4_

  - [ ]* 8.2 Write property tests for properties reader
    - **Property 14: Properties File Ordering**
    - **Property 15: Alphabetical Fallback Ordering**
    - **Validates: Requirements 10.1–10.4**

- [x] 9. Doc generator module
  - [x] 9.1 Create Jinja2 Markdown templates
    - Create `src/templates/pipeline.md.j2` — per-pipeline template with Job Overview, Connections, Sources, Transformations, Lineage, Outputs, Variable Usage sections
    - Create `src/templates/consolidated.md.j2` — multi-file template with Table of Contents, per-pipeline sections, cross-pipeline summary
    - Use hierarchical headings, tables, and code blocks for readability
    - _Requirements: 12.1, 12.2_

  - [x] 9.2 Implement doc generator core
    - Create `src/doc_generator.py` with `generate_documentation(models, sequence)` function
    - Render Job Overview: job name, parameters with keys/values, domain folder
    - Render Connections: numeric ID, database type (derived from driver), URL, masked passwords (asterisks)
    - Render Sources: source ID, query/dbtable/path depending on type, connection reference with resolved database type
    - _Requirements: 4.1, 4.2, 4.3, 5.1, 5.2, 5.3, 5.4, 6.1, 6.2, 6.3, 6.4, 6.5_

  - [x] 9.3 Implement transformation documentation
    - Render map transformations: column mappings, expressions, srcFilter, LITERAL values
    - Render join transformations: join type, sourceA, sourceB, join expressions (AND and OR), srcAFilter, srcBFilter
    - Render union transformations: sourceA, sourceB, aligned column mappings for each side
    - Render agg transformations: group-by columns, aggregation expressions, sort order
    - Present transformations in topological dependency order
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9, 7.10, 7.11_

  - [x] 9.4 Implement lineage and output documentation
    - Render Data Lineage section: directed flow from sources through derived to outputs
    - Render complete dependency chains for each output back to source nodes
    - Render Outputs section: dataframe ID, table/path, format, mode, batchsize, connection
    - Handle multi-output: list each target separately, indicate same dataframe feeds multiple destinations
    - _Requirements: 8.1, 8.2, 9.1, 9.2, 9.3, 9.4, 9.5_

  - [x] 9.5 Implement variable usage and multi-file documentation
    - Render Variable Usage section: list variables with default values, cross-reference table, unresolved variable warnings
    - Render multi-file consolidated doc: table of contents grouped by domain, individual pipeline sections, summary with totals
    - Apply execution sequence ordering from properties files; fallback to alphabetical with warning
    - _Requirements: 10.2, 10.3, 10.4, 11.1, 11.2, 11.3, 13.1, 13.2, 13.3, 13.4, 13.5_

  - [ ]* 9.6 Write property tests for doc generator
    - **Property 4: Password Masking**
    - **Property 5: Job Overview Documentation Completeness**
    - **Property 6: Connection Documentation Completeness**
    - **Property 7: Source Documentation Completeness**
    - **Property 8: Transformation Documentation Completeness**
    - **Property 9: Join Documentation Completeness**
    - **Property 12: Output Documentation Completeness**
    - **Property 13: Multi-Output Indication**
    - **Property 16: Multi-File Consolidated Documentation**
    - **Validates: Requirements 4.1–4.3, 5.1–5.4, 6.1–6.5, 7.1–7.11, 9.1–9.5, 11.1–11.3**

- [x] 10. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. Flask web application
  - [x] 11.1 Implement upload endpoint
    - Create `src/app.py` with Flask application
    - Implement `POST /upload`: accept multiple JSON files, validate extensions, validate schemas, parse pipelines, generate documentation
    - Return confirmation listing accepted filenames on success
    - Return 400 with descriptive errors on validation failure
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

  - [x] 11.2 Implement preview and download endpoints
    - Implement `GET /preview`: render generated Markdown as HTML within the application interface
    - Implement `GET /download`: serve generated Markdown file for download
    - _Requirements: 12.3, 12.4_

  - [x] 11.3 Implement file upload interface
    - Create HTML template for file upload form accepting multiple JSON files
    - Display confirmation with accepted filenames after successful upload
    - Display error messages for invalid files
    - _Requirements: 1.1, 1.5_

- [ ] 12. Hypothesis test strategies
  - [ ]* 12.1 Implement shared Hypothesis strategies
    - Create `tests/strategies.py` with custom strategies
    - Implement `pipeline_json_strategy()`: generates valid Pipeline_JSON dicts with structurally correct content
    - Implement `source_dataframe_strategy()`: generates sources with valid type/connection/query combinations
    - Implement `derived_dataframe_strategy()`: generates derived dataframes with valid transformation metadata
    - Implement `column_mapping_strategy()`: generates valid col() and expr() patterns including LITERAL values
    - Implement `variable_strategy()`: generates variable definitions and matching ${var} references
    - Configure settings: max_examples=200, suppress HealthCheck.too_slow
    - _Requirements: 3.2_

- [ ] 13. Integration tests
  - [ ]* 13.1 Write integration tests with real JSON files
    - Create `tests/test_integration.py`
    - Test full upload→parse→generate→download flow with real JSON files from jsons/ directory
    - Test cross-pipeline lineage detection with known matching table names across domain files
    - Test multi-file consolidated documentation with files from multiple domain folders
    - Test properties file ordering with domain folder sequences
    - _Requirements: 1.1, 8.3, 10.1, 11.1, 11.2, 11.3_

- [x] 14. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties (18 properties total)
- Unit tests validate specific examples and edge cases
- The design specifies Python 3.11+ with Flask, dataclasses, Jinja2, Hypothesis, and pytest
