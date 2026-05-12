# Requirements Document

## Introduction

The Pipeline Optimizer feature automatically applies optimization recommendations from the analyzer to produce a shorter, cleaner Pipeline_JSON that is semantically equivalent to the original. It also generates human-readable documentation of the changes and an automated equivalence proof explaining why the pre and post JSONs produce the same result.

The system operates on the internal PipelineModel representation (parsed from Pipeline_JSON), applies transformations, serializes back via the pretty_printer, and produces both the optimized JSON and supporting documentation.

## Glossary

- **Optimizer**: The engine module (`src/optimizer.py`) that applies transformation rules to a PipelineModel to produce a reduced, equivalent PipelineModel
- **Pipeline_JSON**: The JSON format used to define data pipelines, containing source dataframes, derived dataframes, joins, maps, aggregations, and outputs
- **PipelineModel**: The internal Python dataclass representation of a Pipeline_JSON, as defined in `src/models.py`
- **Analyzer**: The existing module (`src/analyzer.py`) that detects optimization opportunities and produces PipelineRecommendations
- **PipelineRecommendations**: The dataclass output of the Analyzer containing duplicate joins, duplicate maps, collapse candidates, and general notes
- **Equivalence_Proof**: A structured Markdown document that demonstrates the optimized pipeline produces the same output as the original
- **Optimization_Report**: A Markdown document showing before/after comparison and a summary of applied transformations
- **Pass_Through_Map**: A map dataframe that only copies columns from its source without applying filters or expressions
- **Chained_Maps**: Two or more consecutive map dataframes where the downstream map reads from the upstream map, and neither has filters
- **Source_Pair_Consolidation**: Merging multiple join dataframes that join the same two source dataframes into a single join
- **Column_Ordering**: The sequence of columns in a dataframe's output, which must be preserved during optimization
- **Round_Trip**: The property that `parse(print(model)) == model` for any valid PipelineModel

## Requirements

### Requirement 1: Inline Pass-Through Maps

**User Story:** As a pipeline developer, I want pass-through maps to be automatically inlined into their consuming dataframe, so that the pipeline has fewer unnecessary intermediate steps.

#### Acceptance Criteria

1. WHEN the Analyzer identifies a Pass_Through_Map with a single consumer, THE Optimizer SHALL replace all column references to the Pass_Through_Map in the consumer with direct references to the Pass_Through_Map's source dataframe
2. WHEN a Pass_Through_Map is inlined, THE Optimizer SHALL remove the Pass_Through_Map from the PipelineModel's derived list
3. WHEN a Pass_Through_Map is inlined, THE Optimizer SHALL preserve the Column_Ordering of the consumer dataframe's output
4. IF a Pass_Through_Map has a srcFilter, THEN THE Optimizer SHALL retain the dataframe without inlining

### Requirement 2: Collapse Chained Maps

**User Story:** As a pipeline developer, I want consecutive map steps without filters to be collapsed into a single map, so that the pipeline is more concise and readable.

#### Acceptance Criteria

1. WHEN two Chained_Maps are detected (map A feeds map B, neither has filters), THE Optimizer SHALL merge the column mappings into a single map dataframe
2. WHEN collapsing Chained_Maps, THE Optimizer SHALL resolve transitive column references so that the merged map references the original source dataframe directly
3. WHEN collapsing Chained_Maps, THE Optimizer SHALL preserve the Column_Ordering of the downstream map's output
4. IF either map in a chain has a srcFilter, THEN THE Optimizer SHALL retain both maps without collapsing
5. WHEN collapsing Chained_Maps, THE Optimizer SHALL preserve expression columns from the upstream map by substituting them into the downstream map's references

### Requirement 3: Consolidate Multiple Joins Between Same Source Pair

**User Story:** As a pipeline developer, I want multiple joins between the same two dataframes to be consolidated into a single join, so that redundant join operations are eliminated.

#### Acceptance Criteria

1. WHEN the Analyzer identifies multiple joins between the same source pair (sourceA, sourceB), THE Optimizer SHALL merge them into a single join dataframe that selects all needed columns
2. WHEN consolidating joins, THE Optimizer SHALL combine the join expressions using AND logic
3. WHEN consolidating joins, THE Optimizer SHALL preserve the join type (inner or left) of each original join
4. IF the multiple joins have different join types, THEN THE Optimizer SHALL retain them as separate joins without consolidation
5. WHEN consolidating joins, THE Optimizer SHALL update all downstream references to point to the consolidated join dataframe

### Requirement 4: Remove Unused Source Dataframes

**User Story:** As a pipeline developer, I want unused source dataframes to be automatically removed, so that the pipeline JSON does not contain dead configuration.

#### Acceptance Criteria

1. WHEN a source dataframe is not referenced by any derived dataframe (directly or transitively), THE Optimizer SHALL remove the source dataframe from the PipelineModel
2. WHEN removing an unused source dataframe, THE Optimizer SHALL also remove its associated connection entry if no other source uses that connection
3. WHEN removing an unused source dataframe, THE Optimizer SHALL also remove its associated srcDFsOptionsMP entry

### Requirement 5: Merge Duplicate Column Mapping Patterns

**User Story:** As a pipeline developer, I want dataframes with identical column mapping patterns to be merged into a single shared dataframe, so that duplication is eliminated.

#### Acceptance Criteria

1. WHEN the Analyzer identifies duplicate map patterns (same source columns, same expressions), THE Optimizer SHALL replace the duplicates with a single map dataframe
2. WHEN merging duplicate maps, THE Optimizer SHALL update all consumers of the removed duplicates to reference the retained map dataframe
3. WHEN merging duplicate maps, THE Optimizer SHALL preserve the Column_Ordering expected by each consumer
4. IF duplicate maps have different srcFilters, THEN THE Optimizer SHALL retain them as separate dataframes without merging

### Requirement 6: Optimization Orchestration

**User Story:** As a pipeline developer, I want to run the optimizer on any Pipeline_JSON file and receive the optimized JSON output, so that I can use the shorter pipeline in production.

#### Acceptance Criteria

1. THE Optimizer SHALL accept a PipelineModel as input and return an optimized PipelineModel as output
2. THE Optimizer SHALL invoke the Analyzer to obtain PipelineRecommendations before applying transformations
3. WHEN multiple optimization rules apply to the same pipeline, THE Optimizer SHALL apply them in a safe order: remove unused sources first, then inline pass-throughs, then collapse chains, then consolidate joins, then merge duplicates
4. THE Optimizer SHALL produce a valid PipelineModel that can be serialized by the pretty_printer without errors
5. WHEN no optimizations are applicable, THE Optimizer SHALL return the original PipelineModel unchanged

### Requirement 7: Optimized JSON Serialization

**User Story:** As a pipeline developer, I want the optimized PipelineModel to be serialized back to valid Pipeline_JSON, so that I can use it as a drop-in replacement for the original.

#### Acceptance Criteria

1. THE Optimizer SHALL serialize the optimized PipelineModel using the existing pretty_printer module
2. THE Optimizer SHALL produce Pipeline_JSON that conforms to the same schema as the input
3. FOR ALL valid PipelineModel instances, parsing the optimized JSON then printing it SHALL produce an equivalent JSON (round-trip property)

### Requirement 8: Optimization Report Generation

**User Story:** As a pipeline developer, I want a Markdown report showing what the optimizer changed, so that I can review and understand the optimizations applied.

#### Acceptance Criteria

1. WHEN optimization is complete, THE Optimizer SHALL generate an Optimization_Report in Markdown format
2. THE Optimization_Report SHALL include a summary section listing the number of dataframes removed, merged, or inlined
3. THE Optimization_Report SHALL include a before/after comparison showing the original step count versus the optimized step count
4. THE Optimization_Report SHALL include a detailed section listing each applied transformation with the affected dataframe IDs and the rule that triggered it
5. WHEN no optimizations are applied, THE Optimization_Report SHALL state that the pipeline is already optimal

### Requirement 9: Equivalence Proof Generation

**User Story:** As a pipeline developer, I want automated reasoning that proves the optimized JSON produces the same result as the original, so that I can trust the optimization is safe.

#### Acceptance Criteria

1. WHEN optimization is complete, THE Optimizer SHALL generate an Equivalence_Proof in Markdown format
2. THE Equivalence_Proof SHALL demonstrate that the same source data is read by listing all source dataframes and their queries in both the original and optimized pipelines
3. THE Equivalence_Proof SHALL demonstrate that the same transformations are applied by showing a step-by-step mapping from original steps to optimized steps
4. THE Equivalence_Proof SHALL demonstrate that the same output columns and values are produced by comparing the final output dataframe's column list and expressions
5. THE Equivalence_Proof SHALL include a reasoning chain for each optimization rule applied, explaining why the transformation preserves semantics
6. THE Equivalence_Proof SHALL verify that join semantics (join type and join expressions) are preserved for all join operations
7. THE Equivalence_Proof SHALL verify that filter conditions are preserved (either in the same position or moved to an equivalent position in the pipeline)
8. THE Equivalence_Proof SHALL use the round-trip property (parse → optimize → print → parse produces equivalent model) as a structural validation step

### Requirement 10: Column Ordering Preservation

**User Story:** As a pipeline developer, I want the optimized pipeline to produce output columns in the same order as the original, so that downstream consumers are not affected.

#### Acceptance Criteria

1. THE Optimizer SHALL preserve the column order of the final output dataframe after all optimizations are applied
2. WHEN inlining or merging dataframes, THE Optimizer SHALL rewrite column references to maintain the original positional order in the consumer
3. IF an optimization would change the output column order, THEN THE Optimizer SHALL add an explicit column reordering step to restore the original order

### Requirement 11: Pretty Printer Round-Trip Validation

**User Story:** As a pipeline developer, I want to verify that the optimized JSON can be parsed back into an equivalent model, so that no information is lost during serialization.

#### Acceptance Criteria

1. THE Optimizer SHALL validate the round-trip property by parsing the serialized optimized JSON and comparing it to the optimized PipelineModel
2. IF the round-trip validation fails, THEN THE Optimizer SHALL report the discrepancy and return the original PipelineModel without optimization
3. FOR ALL valid Pipeline_JSON inputs, THE Optimizer SHALL guarantee that `parse(print(optimize(parse(json)))) == optimize(parse(json))`
