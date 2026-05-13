# Implementation Plan: Spark DAG Generator

## Overview

Implement a Spark DAG generator that transforms optimized PipelineModels into typed SparkDAG intermediate representations. The implementation follows a bottom-up approach: data models first, then core generation logic, traversal algorithms, serialization, rendering, and finally Flask integration.

## Tasks

- [x] 1. Implement SparkDAG data models
  - [x] 1.1 Create `src/spark_dag_models.py` with all enums and dataclasses
    - Define `SparkOperation` enum with all 8 operation types
    - Define all metadata dataclasses: `ReadMetadata`, `SelectMetadata`, `FilterMetadata`, `JoinMetadata`, `UnionMetadata`, `GroupByAggMetadata`, `SortMetadata`, `WriteMetadata`
    - Define `MetadataType` union type
    - Define `SparkNode` dataclass with id, operation, metadata, source_df_name
    - Define `EdgeType` enum (FEEDS, JOINS_LEFT, JOINS_RIGHT, UNIONS_LEFT, UNIONS_RIGHT)
    - Define `SparkEdge` dataclass with from_node_id, to_node_id, edge_type
    - Define `SparkDAG` dataclass with pipeline_name, nodes, edges, and helper methods (get_node, get_children, get_parents)
    - _Requirements: 1.1, 1.2, 2.1_

- [-] 2. Implement DAG Generator Core
  - [x] 2.1 Create `src/spark_dag_generator.py` with source and output node generation
    - Implement `generate_spark_dag(model: PipelineModel) -> SparkDAG` function skeleton
    - Implement SourceDataframe → SparkRead node creation with connection resolution
    - Implement OutputTarget → SparkWrite node creation with connection resolution
    - Implement edge creation from parent nodes to child nodes
    - _Requirements: 1.3, 1.10, 9.2, 9.4_

  - [x] 2.2 Implement MAP decomposition logic
    - MAP without src_filter → SparkSelect node
    - MAP with src_filter → SparkFilter → SparkSelect (with edges source→filter→select)
    - Populate SelectMetadata.columns from DerivedDataframe column mappings
    - Populate FilterMetadata.conditions from src_filter list
    - _Requirements: 1.4, 1.5, 2.2_

  - [x] 2.3 Implement JOIN decomposition logic
    - JOIN → SparkJoin node with two incoming edges (left, right)
    - JOIN with src_a_filter → SparkFilter(A) → SparkJoin
    - JOIN with src_b_filter → SparkFilter(B) → SparkJoin
    - Populate JoinMetadata with join_type, conditions, left/right input IDs
    - Create edges with JOINS_LEFT and JOINS_RIGHT edge types
    - _Requirements: 1.6, 2.4_

  - [x] 2.4 Implement UNION decomposition logic
    - UNION → SparkUnion node with two incoming edges (left, right)
    - Populate UnionMetadata with left/right input IDs and column mappings
    - Create edges with UNIONS_LEFT and UNIONS_RIGHT edge types
    - _Requirements: 1.7, 2.5_

  - [x] 2.5 Implement AGG decomposition logic
    - AGG without sort → SparkGroupByAgg node
    - AGG with sort → SparkGroupByAgg → SparkSort (with edges agg→sort→output)
    - Populate GroupByAggMetadata with group_by_columns and aggregations
    - Populate SortMetadata with columns and direction
    - _Requirements: 1.8, 1.9, 2.3_

  - [x] 2.6 Implement `generate_spark_dags(result: OptimizationResult) -> list[SparkDAG]`
    - Accept OptimizationResult and generate one SparkDAG per PipelineModel
    - Implement DAG acyclicity validation
    - Implement error handling for missing sources, missing connections, empty pipelines
    - _Requirements: 9.1, 9.3, 2.6_

  - [ ]* 2.7 Write property tests for DAG generation (Properties 1-6, 14, 15)
    - **Property 1: Node Uniqueness Invariant** — all node IDs unique, non-null operation and metadata
    - **Validates: Requirements 1.1**
    - **Property 2: Correct Node Generation Per Element Type** — each element type produces correct node type with matching metadata
    - **Validates: Requirements 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 1.10**
    - **Property 3: Filter Insertion Pattern** — MAP with src_filter produces source→filter→select edge pattern
    - **Validates: Requirements 2.2**
    - **Property 4: Sort Insertion Pattern** — AGG with sort produces agg→sort→write edge pattern
    - **Validates: Requirements 2.3**
    - **Property 5: Multi-Input Nodes Have Exactly Two Incoming Edges** — Join/Union nodes have exactly 2 parents
    - **Validates: Requirements 2.4, 2.5**
    - **Property 6: DAG Acyclicity** — topological sort always succeeds
    - **Validates: Requirements 2.6**
    - **Property 14: DAG Count Matches Model Count** — N PipelineModels → N SparkDAGs
    - **Validates: Requirements 9.1, 9.3**
    - **Property 15: Connection Resolution** — Read/Write nodes have connection details populated from model
    - **Validates: Requirements 9.4**
    - Create Hypothesis strategies: `st_pipeline_model()`, `st_derived_dataframe(type)`, `st_column_mapping()`
    - File: `tests/test_spark_dag_generator.py`

- [ ] 3. Checkpoint - Ensure DAG generation tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 4. Implement Traversal Engine
  - [x] 4.1 Create `src/spark_dag_traversal.py` with topological order
    - Implement `topological_order(dag: SparkDAG) -> list[str]` using Kahn's algorithm
    - Ensure deterministic ordering: nodes at same depth sorted alphabetically by ID
    - Raise `ValueError` with cycle node IDs if cycle detected
    - _Requirements: 3.1, 3.2, 3.3_

  - [x] 4.2 Implement reverse path traversal
    - Implement `reverse_paths(dag: SparkDAG, output_node_id: str) -> list[list[str]]`
    - Return all paths from output to source nodes (output first, source last)
    - Handle multiple paths through joins/unions
    - Raise `KeyError` for non-existent node IDs
    - _Requirements: 4.1, 4.2, 4.3_

  - [x] 4.3 Implement level-based traversal
    - Implement `level_order(dag: SparkDAG) -> list[list[str]]`
    - Level 0 = source (SparkRead) nodes
    - Each node's level = longest path from any source to that node
    - Return contiguous levels from 0 to max depth
    - _Requirements: 5.1, 5.2, 5.3_

  - [ ]* 4.4 Write property tests for traversal (Properties 7-10)
    - **Property 7: Topological Order Respects Edges** — for every edge (u,v), u appears before v
    - **Validates: Requirements 3.1**
    - **Property 8: Topological Order Is Deterministic** — multiple calls produce same result, alphabetical at same depth
    - **Validates: Requirements 3.2**
    - **Property 9: Reverse Paths From Output to Sources** — paths start at output, end at SparkRead, consecutive pairs connected by edges
    - **Validates: Requirements 4.1, 4.2, 4.3**
    - **Property 10: Level Assignment Correctness** — level 0 = reads, level = longest path from source, contiguous levels, every node in exactly one level
    - **Validates: Requirements 5.1, 5.2, 5.3**
    - Create Hypothesis strategy: `st_spark_dag()` for generating valid SparkDAGs
    - File: `tests/test_spark_dag_traversal.py`

- [ ] 5. Checkpoint - Ensure traversal tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 6. Implement JSON Serializer
  - [x] 6.1 Create `src/spark_dag_serializer.py` with serialize function
    - Implement `serialize_dag(dag: SparkDAG) -> dict`
    - Serialize nodes with id, operation, source_df_name, and full metadata
    - Serialize edges with from_node_id, to_node_id, edge_type
    - Include execution_order (topological) and levels (level-based) in output
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

  - [x] 6.2 Implement deserialize function
    - Implement `deserialize_dag(data: dict) -> SparkDAG`
    - Reconstruct SparkNodes with correct metadata types based on operation discriminator
    - Reconstruct SparkEdges with correct EdgeType
    - Raise `ValueError` for missing/invalid fields
    - _Requirements: 6.6_

  - [ ]* 6.3 Write property tests for serialization (Properties 11-12)
    - **Property 11: Serialization Round-Trip** — serialize then deserialize produces equivalent SparkDAG
    - **Validates: Requirements 6.6**
    - **Property 12: Serialized JSON Structure** — output contains required keys (nodes, edges, execution_order, levels, pipeline_name) with correct types
    - **Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5**
    - File: `tests/test_spark_dag_serializer.py`

- [ ] 7. Implement Mermaid Renderer and Markdown Report
  - [x] 7.1 Create `src/spark_dag_renderer.py` with Mermaid rendering
    - Implement `render_mermaid(dag: SparkDAG) -> str`
    - Start with "graph TD"
    - Use distinct shapes: rectangles `[]` for reads, rounded `()` for transforms, diamonds `{}` for joins, hexagons `{{}}` for outputs
    - Label nodes with operation type and identifier
    - Label edges with relationship type (feeds, joins_left, joins_right, unions_left, unions_right)
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_

  - [x] 7.2 Implement Markdown report generation
    - Implement `render_markdown_report(dag: SparkDAG) -> str`
    - Include title with pipeline name
    - Include Mermaid flowchart in fenced code block
    - Include topological execution order as numbered list
    - Include node details table (ID, operation type, key metadata summary)
    - Include level-based grouping section showing parallel execution opportunities
    - _Requirements: 10.1, 10.2, 10.3_

  - [ ]* 7.3 Write property tests for rendering (Properties 13, 16)
    - **Property 13: Mermaid Contains All Nodes and Edges** — output starts with "graph TD", contains all nodes with shapes, contains all edges with labels
    - **Validates: Requirements 7.1, 7.2, 7.3, 7.4**
    - **Property 16: Markdown Report Completeness** — contains Mermaid block, numbered topological list, node details table, level grouping
    - **Validates: Requirements 10.1, 10.2, 10.3**
    - File: `tests/test_spark_dag_renderer.py`

- [ ] 8. Checkpoint - Ensure serializer and renderer tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 9. Implement Flask Integration
  - [x] 9.1 Add Spark DAG endpoints to `src/app.py`
    - Add `GET /spark-dag` endpoint rendering upload form for JSON pipeline files
    - Add `POST /spark-dag` endpoint that parses, validates, optimizes, and generates SparkDAG from uploaded files
    - Add `GET /spark-dag/download/json` endpoint returning serialized SparkDAG as downloadable JSON (Content-Type: application/json)
    - Add `GET /spark-dag/download/markdown` endpoint returning Markdown report as downloadable file
    - Return HTTP 404 with descriptive message when download endpoints accessed before DAG generation
    - Return HTTP 400 with validation error list for invalid uploads
    - Return HTTP 400 with "No files provided" for empty file submissions
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6_

  - [ ]* 9.2 Write integration tests for Flask endpoints and real pipeline data
    - Test upload flow with real JSON files from `jsons/` directory
    - Test download JSON endpoint returns valid serialized DAG
    - Test download Markdown endpoint returns valid report
    - Test error responses (404 before generation, 400 for invalid input)
    - Test end-to-end: upload `ap_aging_complex.json` → verify DAG structure matches expected decomposition
    - File: `tests/test_spark_dag_integration.py`
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 9.1, 9.2_

- [x] 10. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- The design uses Python with dataclasses, Hypothesis for property testing, and Flask for endpoints
- Each property test task references specific correctness properties from the design document
- Checkpoints ensure incremental validation between major implementation phases
- All modules follow the existing project pattern of pure-function modules with dataclass models
- Add `hypothesis>=6.0` and `pytest>=7.0` to `requirements.txt` when implementing test tasks
