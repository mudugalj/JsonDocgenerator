# Implementation Plan: Pipeline Optimizer

## Overview

Implement a transformation engine that applies semantics-preserving optimization rules to PipelineModel instances, generates optimization reports and equivalence proofs, and integrates with the Flask web app. The implementation follows a bottom-up approach: data models and utilities first, then individual rules, orchestrator, cross-pipeline logic, documentation generators, and finally Flask integration.

## Tasks

- [x] 1. Data models and utilities
  - [x] 1.1 Create `src/optimizer.py` with data model classes
    - Define `OptimizationStep`, `CrossPipelineMerge`, `OptimizationResult`, and `ProofStep` dataclasses
    - Define `OptimizationRule` Protocol class with `name`, `applies()`, and `apply()` methods
    - Import required types from `src/models.py`
    - _Requirements: 6.1, 6.4_

  - [x] 1.2 Implement `deep_copy_model` utility
    - Create a function that produces an independent deep copy of a `PipelineModel` and all nested dataclasses
    - Ensure no shared mutable references between original and copy
    - _Requirements: 6.1_

  - [x] 1.3 Implement `rewrite_column_ref` utility
    - Create a function that takes a `ColumnMapping`, `old_source_id`, and `new_source_id`, and returns a new `ColumnMapping` with `source_df` replaced
    - Handle rewriting of `raw` field if it contains the old source ID
    - _Requirements: 1.1, 2.2, 3.5, 10.2_

- [x] 2. Optimization rule: InlinePassThrough
  - [x] 2.1 Implement `InlinePassThrough` rule class
    - Implement `applies()`: check if analyzer recommendations identify pass-through maps with single consumers and no srcFilter
    - Implement `apply()`: replace column references in the consumer, remove the pass-through from derived list, preserve column ordering
    - Return `OptimizationStep` records for each inlined map
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

  - [ ]* 2.2 Write property test for InlinePassThrough (Property 1)
    - **Property 1: Inline pass-through removes node and rewrites references**
    - **Validates: Requirements 1.1, 1.2**

  - [ ]* 2.3 Write property test for InlinePassThrough column order (Property 2)
    - **Property 2: Inline preserves consumer column order**
    - **Validates: Requirements 1.3**

  - [ ]* 2.4 Write property test for filter prevention (Property 3)
    - **Property 3: Pass-throughs with filters are not inlined**
    - **Validates: Requirements 1.4**

- [x] 3. Optimization rule: CollapseChainedMaps
  - [x] 3.1 Implement `CollapseChainedMaps` rule class
    - Implement `applies()`: detect chained maps (A feeds B, neither has filters) from analyzer recommendations
    - Implement `apply()`: merge column mappings, resolve transitive references, substitute expressions from upstream into downstream, preserve downstream column order
    - Return `OptimizationStep` records for each collapsed chain
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5_

  - [ ]* 3.2 Write property test for chain collapse (Property 4)
    - **Property 4: Chained maps collapse with transitive reference resolution**
    - **Validates: Requirements 2.1, 2.2**

  - [ ]* 3.3 Write property test for collapsed chain column order (Property 5)
    - **Property 5: Collapsed chain preserves downstream column order**
    - **Validates: Requirements 2.3**

  - [ ]* 3.4 Write property test for filter prevention (Property 6)
    - **Property 6: Chains with filters are not collapsed**
    - **Validates: Requirements 2.4**

  - [ ]* 3.5 Write property test for expression substitution (Property 7)
    - **Property 7: Expression substitution in collapsed chains**
    - **Validates: Requirements 2.5**

- [x] 4. Optimization rule: ConsolidateJoins
  - [x] 4.1 Implement `ConsolidateJoins` rule class
    - Implement `applies()`: detect multiple joins between same source pair with same join type
    - Implement `apply()`: merge into single join with union of columns and AND-combined join expressions, update downstream references
    - Return `OptimizationStep` records for each consolidation
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

  - [ ]* 4.2 Write property test for join consolidation (Property 8)
    - **Property 8: Duplicate joins consolidated with combined expressions**
    - **Validates: Requirements 3.1, 3.2, 3.3**

  - [ ]* 4.3 Write property test for different join types (Property 9)
    - **Property 9: Different join types prevent consolidation**
    - **Validates: Requirements 3.4**

  - [ ]* 4.4 Write property test for downstream reference update (Property 10)
    - **Property 10: Consolidated join updates downstream references**
    - **Validates: Requirements 3.5**

- [x] 5. Optimization rule: RemoveUnusedSources
  - [x] 5.1 Implement `RemoveUnusedSources` rule class
    - Implement `applies()`: detect source dataframes not referenced by any derived dataframe
    - Implement `apply()`: remove unused sources, their srcDFsOptionsMP entries, and connection entries if no other source uses that connection
    - Return `OptimizationStep` records for each removed source
    - _Requirements: 4.1, 4.2, 4.3_

  - [ ]* 5.2 Write property test for unused source removal (Property 11)
    - **Property 11: Unused sources removed with all associated entries**
    - **Validates: Requirements 4.1, 4.2, 4.3**

- [x] 6. Optimization rule: MergeDuplicateMaps
  - [x] 6.1 Implement `MergeDuplicateMaps` rule class
    - Implement `applies()`: detect map dataframes with identical column mapping patterns (same source columns, same expressions, same filters)
    - Implement `apply()`: retain one map, remove duplicates, update all consumers to reference the retained map, preserve column ordering per consumer
    - Return `OptimizationStep` records for each merge
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

  - [ ]* 6.2 Write property test for duplicate map merge (Property 12)
    - **Property 12: Duplicate maps merged and consumers updated**
    - **Validates: Requirements 5.1, 5.2**

  - [ ]* 6.3 Write property test for different filters (Property 13)
    - **Property 13: Duplicate maps with different filters not merged**
    - **Validates: Requirements 5.4**

- [x] 7. Checkpoint - Core rules complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Optimization orchestrator
  - [x] 8.1 Implement the `optimize()` function
    - Accept `models: list[PipelineModel]` and optional `sequence: list[str]`
    - Deep copy input models before transformation
    - Invoke analyzer to obtain `PipelineRecommendations` for each model
    - Apply rules in safe order: RemoveUnusedSources → InlinePassThrough → CollapseChainedMaps → ConsolidateJoins → MergeDuplicateMaps
    - Apply each pass iteratively until fixed-point (no more changes)
    - Wrap each rule's `apply()` in try/except, skip on failure and record warning
    - Validate round-trip property after optimization; revert to original on failure
    - Return `OptimizationResult` with all steps, models, report, and proof
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 7.1, 7.2, 7.3, 11.1, 11.2, 11.3_

  - [ ]* 8.2 Write property test for serializable output (Property 14)
    - **Property 14: Optimizer output is serializable**
    - **Validates: Requirements 6.4**

  - [ ]* 8.3 Write property test for no-op on optimal pipelines (Property 15)
    - **Property 15: No-op on already-optimal pipelines**
    - **Validates: Requirements 6.5**

  - [ ]* 8.4 Write property test for schema conformance (Property 16)
    - **Property 16: Output JSON conforms to schema**
    - **Validates: Requirements 7.2**

  - [ ]* 8.5 Write property test for round-trip (Property 17)
    - **Property 17: Round-trip property**
    - **Validates: Requirements 7.3, 11.1, 11.3**

  - [ ]* 8.6 Write property test for output column order (Property 20)
    - **Property 20: Final output column order preserved**
    - **Validates: Requirements 10.1, 10.2, 5.3**

- [x] 9. Cross-pipeline optimizer
  - [x] 9.1 Implement `CrossPipelineOptimizer` class
    - Implement `detect_merge_candidates()`: given models and sequence, identify where one pipeline's output feeds another's source
    - Implement `merge_pipelines()`: remove intermediate write/read, connect producer's final derived directly to consumer, merge connections/variables/sources, handle ID conflicts by prefixing
    - Integrate with orchestrator: apply cross-pipeline collapse after individual pipeline optimizations (only when sequence is provided)
    - _Requirements: 6.3_

  - [x] 9.2 Implement `CollapseCrossPipeline` rule class
    - Wrap `CrossPipelineOptimizer.merge_pipelines()` in the `OptimizationRule` protocol interface
    - Record `CrossPipelineMerge` entries and `OptimizationStep` records
    - _Requirements: 6.3_

- [x] 10. Report generator
  - [x] 10.1 Implement `generate_optimization_report()` function
    - Generate Markdown with summary section: count of dataframes removed, merged, inlined
    - Include before/after comparison: original step count vs optimized step count
    - Include detailed section: each applied transformation with rule name, affected IDs, and description
    - Handle "already optimal" case with appropriate message
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5_

  - [ ]* 10.2 Write property test for report content (Property 18)
    - **Property 18: Report contains required information**
    - **Validates: Requirements 8.2, 8.3, 8.4**

- [x] 11. Equivalence proof generator
  - [x] 11.1 Implement `generate_equivalence_proof()` function
    - Generate Markdown with source equivalence section (same queries, same connections)
    - Include transformation mapping section (step-by-step original to optimized)
    - Include output column comparison section (same columns, same order, same targets)
    - Include reasoning chain for each applied rule (using `ProofStep` data)
    - Include join semantics verification (join type and expressions preserved)
    - Include filter preservation verification
    - Include round-trip validation statement
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8_

  - [ ]* 11.2 Write property test for proof sections (Property 19)
    - **Property 19: Proof contains required sections**
    - **Validates: Requirements 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.7, 9.8**

- [x] 12. Checkpoint - Core engine complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 13. Flask integration
  - [x] 13.1 Add `/optimize` POST endpoint to `src/app.py`
    - Accept uploaded JSON files and optional sequence (from text box)
    - Parse each file, call `optimize()`, serialize results
    - Return optimized JSON in response
    - Handle errors: return 400 for parse failures, 500 for unexpected errors
    - _Requirements: 6.1, 7.1_

  - [x] 13.2 Add `/optimize/preview` GET endpoint
    - Render optimization report as HTML page
    - Display report markdown content
    - _Requirements: 8.1_

  - [x] 13.3 Add `/optimize/download` GET endpoint
    - Package optimized JSON, report markdown, and proof markdown into a zip file
    - Return zip as downloadable response
    - _Requirements: 7.1, 8.1, 9.1_

  - [x] 13.4 Add sequence text box UI element
    - Add a `<textarea>` to the upload form for pasting filenames (one per line)
    - Parse textarea content into ordered list of filenames for cross-pipeline optimization
    - Wire textarea value to the `/optimize` POST request
    - _Requirements: 6.3_

  - [ ]* 13.5 Write integration tests for Flask endpoints
    - Test `/optimize` with valid JSON files returns optimized output
    - Test `/optimize` with sequence triggers cross-pipeline optimization
    - Test `/optimize/preview` renders HTML with report content
    - Test `/optimize/download` returns zip with all artifacts
    - Test error handling: malformed JSON returns 400
    - _Requirements: 6.1, 7.1, 8.1, 9.1_

- [x] 14. Final checkpoint - All components integrated
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties using Hypothesis
- The implementation language is Python, matching the existing codebase and design
- All optimization rules follow the `OptimizationRule` Protocol for consistent composition
- The orchestrator applies rules in safe order with fixed-point iteration per pass
