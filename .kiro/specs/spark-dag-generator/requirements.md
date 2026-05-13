# Requirements Document

## Introduction

This feature generates a structured Spark DAG (Directed Acyclic Graph) from an optimized PipelineModel. The DAG serves as an intermediate representation (IR) where each node represents a Spark DataFrame operation with full metadata, and edges represent data flow. This IR is designed to be consumed by a future Spark Scala code generator that walks the DAG top-to-bottom and emits valid Spark Scala code for each node.

## Glossary

- **DAG_Generator**: The module that transforms an optimized PipelineModel into a SparkDAG
- **SparkDAG**: The complete directed acyclic graph containing SparkNodes and SparkEdges representing the Spark execution plan
- **SparkNode**: A single node in the SparkDAG representing one Spark DataFrame operation
- **SparkEdge**: A directed edge in the SparkDAG representing data flow from a parent DataFrame to a child DataFrame
- **PipelineModel**: The existing internal representation of a parsed and optimized ETL pipeline (from src/models.py)
- **Traversal_Engine**: The component that provides algorithms for walking the SparkDAG in various orders
- **Mermaid_Renderer**: The component that produces Mermaid flowchart syntax from a SparkDAG
- **DAG_Serializer**: The component that converts a SparkDAG to a JSON representation suitable for consumption by a Scala code generator
- **Optimizer**: The existing pipeline optimizer (src/optimizer.py) that produces optimized PipelineModels

## Requirements

### Requirement 1: SparkDAG Model

**User Story:** As a developer, I want a structured DAG model with typed nodes representing Spark operations, so that a code generator can walk the graph and emit Spark Scala code for each node.

#### Acceptance Criteria

1. THE DAG_Generator SHALL represent the SparkDAG as a collection of SparkNodes and SparkEdges where each SparkNode has a unique identifier, an operation type, and operation-specific metadata
2. THE DAG_Generator SHALL support the following SparkNode operation types: SparkRead, SparkSelect, SparkFilter, SparkJoin, SparkUnion, SparkGroupByAgg, SparkSort, SparkWrite
3. WHEN a SourceDataframe is encountered in the PipelineModel, THE DAG_Generator SHALL create a SparkRead node containing the source format (jdbc, parquet, or hive), connection URL, query or path, and connection credentials reference
4. WHEN a DerivedDataframe of type MAP is encountered, THE DAG_Generator SHALL create a SparkSelect node containing the list of column expressions (col and expr references) with their aliases
5. WHEN a DerivedDataframe has a non-empty src_filter list, THE DAG_Generator SHALL create a SparkFilter node containing the filter condition expressions
6. WHEN a DerivedDataframe of type JOIN is encountered, THE DAG_Generator SHALL create a SparkJoin node containing the join type (inner, left), join condition expressions, and references to the left and right parent DataFrame node identifiers
7. WHEN a DerivedDataframe of type UNION is encountered, THE DAG_Generator SHALL create a SparkUnion node containing references to the left and right parent DataFrame node identifiers and their respective column mappings
8. WHEN a DerivedDataframe of type AGG is encountered, THE DAG_Generator SHALL create a SparkGroupByAgg node containing the groupBy column list and the aggregation expressions with aliases
9. WHEN a DerivedDataframe has a non-empty sort list, THE DAG_Generator SHALL create a SparkSort node containing the sort columns with their ascending or descending direction
10. WHEN an OutputTarget is encountered in the PipelineModel, THE DAG_Generator SHALL create a SparkWrite node containing the output format (jdbc or parquet), target table name or path, write mode, and connection reference

### Requirement 2: DAG Edge Construction

**User Story:** As a developer, I want edges in the DAG to represent data flow between DataFrame operations, so that the traversal order and dependencies are explicit.

#### Acceptance Criteria

1. THE DAG_Generator SHALL create a SparkEdge from each parent SparkNode to each child SparkNode that consumes the parent DataFrame
2. WHEN a SparkFilter node is generated from a src_filter, THE DAG_Generator SHALL insert the SparkFilter node between the source node and the SparkSelect node, creating edges source→filter→select
3. WHEN a SparkSort node is generated from a sort list, THE DAG_Generator SHALL insert the SparkSort node between the aggregation node and the output node, creating edges agg→sort→output
4. WHEN a SparkJoin node is created, THE DAG_Generator SHALL create two incoming edges: one from the left DataFrame node and one from the right DataFrame node
5. WHEN a SparkUnion node is created, THE DAG_Generator SHALL create two incoming edges: one from the left DataFrame node and one from the right DataFrame node
6. THE DAG_Generator SHALL validate that the constructed SparkDAG contains no cycles

### Requirement 3: Topological Traversal

**User Story:** As a developer, I want to traverse the DAG in dependency order (sources first, outputs last), so that a code generator can emit DataFrame declarations in valid execution order.

#### Acceptance Criteria

1. THE Traversal_Engine SHALL provide a topological traversal that returns SparkNodes in dependency order where all parent nodes appear before their children
2. WHEN the SparkDAG contains multiple independent source branches, THE Traversal_Engine SHALL return a deterministic ordering by sorting nodes at the same depth alphabetically by identifier
3. IF a cycle is detected during topological traversal, THEN THE Traversal_Engine SHALL raise an error with the identifiers of the nodes involved in the cycle

### Requirement 4: Reverse Traversal

**User Story:** As a developer, I want to trace from any output node back to its source nodes, so that I can understand the full lineage of a specific output DataFrame.

#### Acceptance Criteria

1. WHEN given an output node identifier, THE Traversal_Engine SHALL return all paths from that output back to source nodes as ordered lists of SparkNode identifiers
2. THE Traversal_Engine SHALL return paths in reverse execution order (output first, source last)
3. WHEN multiple paths exist from an output to sources (due to joins), THE Traversal_Engine SHALL return all distinct paths

### Requirement 5: Level-Based Traversal

**User Story:** As a developer, I want to retrieve all nodes at the same depth level in the DAG, so that I can identify which operations can execute in parallel.

#### Acceptance Criteria

1. THE Traversal_Engine SHALL provide a level-based traversal that groups SparkNodes by their depth in the DAG where source nodes are at level 0
2. THE Traversal_Engine SHALL assign each node a level equal to the length of the longest path from any source node to that node
3. THE Traversal_Engine SHALL return levels as an ordered list of node groups from level 0 to the maximum depth

### Requirement 6: JSON Serialization

**User Story:** As a developer, I want to serialize the SparkDAG to a JSON document, so that a Spark Scala code generator can consume it as input.

#### Acceptance Criteria

1. THE DAG_Serializer SHALL serialize the SparkDAG to a JSON object containing a nodes array and an edges array
2. THE DAG_Serializer SHALL include the topological execution order as a separate ordered list in the JSON output
3. THE DAG_Serializer SHALL include the level assignments for each node in the JSON output
4. THE DAG_Serializer SHALL serialize each SparkNode with its operation type, unique identifier, and the complete operation metadata required to generate Spark code
5. THE DAG_Serializer SHALL serialize each SparkEdge with its source node identifier, target node identifier, and edge relationship type
6. WHEN the JSON is deserialized back into a SparkDAG, THE DAG_Serializer SHALL produce an object equivalent to the original SparkDAG (round-trip property)

### Requirement 7: Mermaid Visualization

**User Story:** As a developer, I want a Mermaid flowchart of the Spark DAG showing operation types on each node, so that I can visually verify the execution plan.

#### Acceptance Criteria

1. THE Mermaid_Renderer SHALL generate a valid Mermaid flowchart (graph TD) from the SparkDAG
2. THE Mermaid_Renderer SHALL label each node with its operation type and identifier (e.g., "SparkRead: oracle_invoices_df")
3. THE Mermaid_Renderer SHALL use distinct node shapes for different operation types: rectangles for reads, rounded rectangles for transforms, diamonds for joins, hexagons for outputs
4. THE Mermaid_Renderer SHALL label edges with the relationship type (feeds, joins_left, joins_right, unions_left, unions_right)
5. THE Mermaid_Renderer SHALL produce syntactically valid Mermaid that renders without errors

### Requirement 8: Flask Integration

**User Story:** As a user, I want a Flask endpoint that accepts optimized pipeline files and returns the Spark DAG as downloadable JSON and a Markdown document with the Mermaid diagram, so that I can use the DAG for Scala code generation.

#### Acceptance Criteria

1. THE Flask application SHALL expose a GET endpoint at /spark-dag that renders an upload form for JSON pipeline files
2. WHEN JSON files are submitted via POST to /spark-dag, THE Flask application SHALL parse, validate, optimize, and generate the SparkDAG from the uploaded files
3. THE Flask application SHALL expose a GET endpoint at /spark-dag/download/json that returns the SparkDAG serialized as a downloadable JSON file with Content-Type application/json
4. THE Flask application SHALL expose a GET endpoint at /spark-dag/download/markdown that returns a Markdown document containing the Mermaid diagram and the topological traversal order
5. IF no SparkDAG has been generated yet, THEN THE Flask application SHALL return HTTP 404 with a descriptive message when download endpoints are accessed
6. IF uploaded files fail validation or parsing, THEN THE Flask application SHALL return HTTP 400 with the list of validation errors

### Requirement 9: Integration with Optimizer Output

**User Story:** As a developer, I want the DAG generator to accept the output of the optimizer directly, so that the optimized pipeline flows seamlessly into DAG generation.

#### Acceptance Criteria

1. THE DAG_Generator SHALL accept an OptimizationResult object and generate a SparkDAG for each optimized PipelineModel contained within it
2. THE DAG_Generator SHALL accept a single PipelineModel and generate a SparkDAG from it
3. WHEN multiple PipelineModels are provided, THE DAG_Generator SHALL generate independent SparkDAGs, one per pipeline
4. THE DAG_Generator SHALL resolve connection details from the PipelineModel connections list when populating SparkRead and SparkWrite node metadata

### Requirement 10: Markdown Report Generation

**User Story:** As a developer, I want a complete Markdown document that includes the Mermaid diagram, traversal order, and node details, so that I have a human-readable reference of the Spark execution plan.

#### Acceptance Criteria

1. THE DAG_Generator SHALL produce a Markdown document containing a title, the Mermaid flowchart diagram, the topological execution order as a numbered list, and a details section for each node
2. THE Markdown document SHALL include a node details table listing each node's identifier, operation type, and key metadata summary
3. THE Markdown document SHALL include the level-based grouping showing which nodes can execute in parallel
