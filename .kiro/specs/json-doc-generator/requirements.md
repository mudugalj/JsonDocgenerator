# Requirements Document

## Introduction

The JSON Doc Generator is a web application that allows users to upload one or more ETL pipeline configuration JSON files and generates human-readable documentation describing the data pipeline flow. The JSON files follow a consistent structure defining sources, transformations (maps, joins, aggregations, unions), and output targets. Sources may be JDBC (Oracle, MySQL), Hive, or Parquet file-based. Users can upload files from multiple domain folders (finance, hr, marketing, sales, supply_chain), and execution sequence across files is defined via properties files associated with each folder.

## Glossary

- **App**: The JSON Doc Generator web application that provides file upload and documentation generation capabilities
- **Pipeline_JSON**: A JSON file conforming to the ETL pipeline configuration schema containing variablesMP, connectionDetailsMP, srcDFsOptionsMP, sourceDfPipeline, derivedDfPipeline, derivedDfPipelineMappingMP, derivedDfPipelineMapSrcMap, derivedDfPipelineJoiSrcMap, outputDfPipeline, tgtDFsMP, and tgtDFsOptionsMP sections
- **Parser**: The component responsible for reading and interpreting Pipeline_JSON files into an internal representation
- **Doc_Generator**: The component responsible for producing human-readable documentation from parsed pipeline data
- **Properties_File**: A configuration file (one per domain folder) that defines the execution sequence of Pipeline_JSON files within that folder
- **Source_Dataframe**: A dataframe definition that reads data from an external connection (JDBC, Hive, or Parquet source)
- **Derived_Dataframe**: A dataframe produced by transforming source or other derived dataframes via map, join, union, or aggregation operations
- **Output_Dataframe**: A dataframe definition that writes results to a target connection (JDBC table or Parquet file)
- **Data_Lineage**: The trace of data flow from source dataframes through transformations to output dataframes
- **Domain_Folder**: A subfolder within the jsons directory representing a business domain (e.g., finance, hr, sales)
- **Union_Dataframe**: A derived dataframe of type "union" that combines two source dataframes (sourceA and sourceB) via UNION ALL with aligned column mappings
- **Parquet_Source**: A source dataframe with type "parquet" that reads from a file path instead of a database query
- **Hive_Source**: A source dataframe with type "hive" that reads from a Hive JDBC connection
- **Variable_Interpolation**: The use of `${variable_name}` syntax within queries and expressions to reference job-level variables defined in variablesMP
- **LITERAL_Expression**: An expression using the `LITERAL(value)` syntax to create constant column values in mappings
- **Pre_Join_Filter**: A filter (srcAFilter or srcBFilter) applied to one side of a join before the join operation executes

## Requirements

### Requirement 1: JSON File Upload

**User Story:** As a user, I want to upload one or more Pipeline_JSON files through the App, so that I can generate documentation for my data pipelines.

#### Acceptance Criteria

1. THE App SHALL provide a file upload interface that accepts one or more JSON files simultaneously
2. WHEN a user uploads files, THE App SHALL validate that each uploaded file has a .json extension
3. WHEN a user uploads files, THE App SHALL validate that each file conforms to the Pipeline_JSON schema structure
4. IF an uploaded file does not conform to the Pipeline_JSON schema, THEN THE App SHALL display a descriptive error message identifying the file name and the missing or malformed section
5. WHEN valid files are uploaded, THE App SHALL display a confirmation listing all accepted file names

### Requirement 2: Pipeline JSON Parsing

**User Story:** As a user, I want the App to correctly parse all sections of my Pipeline_JSON files, so that the generated documentation accurately reflects my pipeline configuration.

#### Acceptance Criteria

1. WHEN a valid Pipeline_JSON file is provided, THE Parser SHALL extract all entries from the variablesMP section including job ID and key-value option pairs
2. WHEN a valid Pipeline_JSON file is provided, THE Parser SHALL extract all connection details from connectionDetailsMP including numeric ID, driver, url, user, and password fields
3. WHEN a valid Pipeline_JSON file is provided, THE Parser SHALL extract all source dataframe options from srcDFsOptionsMP including the numeric ID prefix and query, dbtable, path, or sourceFilter values depending on source type
4. WHEN a valid Pipeline_JSON file is provided, THE Parser SHALL extract all source dataframe definitions from sourceDfPipeline including ID, type (jdbc, hive, or parquet), connection reference, and source reference
5. WHEN a valid Pipeline_JSON file is provided, THE Parser SHALL extract all derived dataframe definitions from derivedDfPipeline including ID and type (map, join, union, or agg)
6. WHEN a valid Pipeline_JSON file is provided, THE Parser SHALL extract column mappings, expressions, filters, join expressions, group-by clauses, aggregations, sort orders, srcAFilter entries, and srcBFilter entries from derivedDfPipelineMappingMP
7. WHEN a valid Pipeline_JSON file is provided, THE Parser SHALL extract source-to-derived mappings from derivedDfPipelineMapSrcMap
8. WHEN a valid Pipeline_JSON file is provided, THE Parser SHALL extract join source mappings from derivedDfPipelineJoiSrcMap including joinType (inner, left, union), sourceA, and sourceB
9. WHEN a valid Pipeline_JSON file is provided, THE Parser SHALL extract output dataframe definitions from outputDfPipeline including target and connection references, recognizing that the same dataframe ID may appear multiple times for multiple output targets
10. WHEN a valid Pipeline_JSON file is provided, THE Parser SHALL extract target format and table name from tgtDFsMP, recognizing that parquet targets may not have a TABLE entry
11. WHEN a valid Pipeline_JSON file is provided, THE Parser SHALL extract target write options from tgtDFsOptionsMP including batchsize, mode, and path values
12. WHEN a valid Pipeline_JSON file is provided containing union-type derived dataframes, THE Parser SHALL extract sourceA and sourceB column mappings from derivedDfPipelineMappingMP entries with IDs suffixed by ".sourceA" and ".sourceB"
13. WHEN a valid Pipeline_JSON file is provided containing join expressions with multiple entries in the Options array, THE Parser SHALL extract all join condition entries as a multi-condition join
14. WHEN a valid Pipeline_JSON file is provided containing LITERAL expressions in column mappings, THE Parser SHALL recognize and extract the `LITERAL(value)` syntax as a constant value assignment
15. WHEN a valid Pipeline_JSON file is provided containing joinExpressionOR entries, THE Parser SHALL extract OR-based join conditions separately from AND-based join conditions

### Requirement 3: Pipeline JSON Pretty Printer

**User Story:** As a developer, I want a pretty printer that can serialize the internal pipeline representation back to valid Pipeline_JSON format, so that round-trip integrity can be verified.

#### Acceptance Criteria

1. THE Pretty_Printer SHALL format internal pipeline representations back into valid Pipeline_JSON files conforming to the original schema
2. FOR ALL valid Pipeline_JSON files, parsing then printing then parsing SHALL produce an equivalent internal representation (round-trip property)

### Requirement 4: Documentation Generation - Job Overview

**User Story:** As a user, I want the generated documentation to include a job overview section, so that I can quickly understand what the pipeline does.

#### Acceptance Criteria

1. WHEN documentation is generated, THE Doc_Generator SHALL produce a Job Overview section containing the job name extracted from variablesMP ID field
2. WHEN documentation is generated, THE Doc_Generator SHALL list all job-level parameters with their keys and values from variablesMP Options
3. WHEN documentation is generated, THE Doc_Generator SHALL identify the domain folder the pipeline belongs to based on the uploaded file path or user selection

### Requirement 5: Documentation Generation - Connection Details

**User Story:** As a user, I want the generated documentation to describe all database connections used, so that I can understand which systems the pipeline interacts with.

#### Acceptance Criteria

1. WHEN documentation is generated, THE Doc_Generator SHALL produce a Connections section listing each connection by its numeric ID
2. WHEN documentation is generated, THE Doc_Generator SHALL display the database type derived from the JDBC driver class name for each connection, recognizing Oracle (oracle.jdbc.OracleDriver), MySQL (com.mysql.cj.jdbc.Driver), and Hive (org.apache.hive.jdbc.HiveDriver) driver types
3. WHEN documentation is generated, THE Doc_Generator SHALL display the connection URL for each connection
4. THE Doc_Generator SHALL mask password values in the generated documentation by replacing them with asterisks

### Requirement 6: Documentation Generation - Source Dataframes

**User Story:** As a user, I want the generated documentation to describe all data sources, so that I can understand where the pipeline reads data from.

#### Acceptance Criteria

1. WHEN documentation is generated, THE Doc_Generator SHALL produce a Sources section listing each source dataframe by its ID
2. WHEN documentation is generated for a JDBC-type source dataframe, THE Doc_Generator SHALL display the SQL query or dbtable reference associated with the source dataframe
3. WHEN documentation is generated for a Hive-type source dataframe, THE Doc_Generator SHALL display the SQL query associated with the source and identify the connection as a Hive JDBC source
4. WHEN documentation is generated for a Parquet-type source dataframe, THE Doc_Generator SHALL display the file path from the srcDFsOptionsMP path value and any sourceFilter condition applied to the parquet data
5. WHEN documentation is generated, THE Doc_Generator SHALL indicate which connection each source dataframe uses by referencing the connection ID and its resolved database type (Oracle, MySQL, Hive, or Parquet file)

### Requirement 7: Documentation Generation - Transformations

**User Story:** As a user, I want the generated documentation to describe all transformations applied, so that I can understand how data is processed from source to target.

#### Acceptance Criteria

1. WHEN documentation is generated for a map-type derived dataframe, THE Doc_Generator SHALL list all column mappings and expressions applied
2. WHEN documentation is generated for a map-type derived dataframe with a srcFilter, THE Doc_Generator SHALL document the filter condition applied to the source
3. WHEN documentation is generated for a join-type derived dataframe, THE Doc_Generator SHALL document the join type, sourceA, sourceB, and join expression
4. WHEN documentation is generated for a join-type derived dataframe with multiple join expression entries, THE Doc_Generator SHALL document all join conditions as a compound AND join (e.g., join on column_a AND column_b)
5. WHEN documentation is generated for a join-type derived dataframe with a joinExpressionOR entry, THE Doc_Generator SHALL document the OR-based join conditions separately from AND-based conditions
6. WHEN documentation is generated for a join-type derived dataframe with a srcAFilter, THE Doc_Generator SHALL document the pre-join filter applied to sourceA before the join executes
7. WHEN documentation is generated for a join-type derived dataframe with a srcBFilter, THE Doc_Generator SHALL document the pre-join filter applied to sourceB before the join executes
8. WHEN documentation is generated for a union-type derived dataframe, THE Doc_Generator SHALL document the union operation including sourceA, sourceB, and the aligned column mappings for each side (sourceA columns and sourceB columns)
9. WHEN documentation is generated for an agg-type derived dataframe, THE Doc_Generator SHALL document the group-by columns, aggregation expressions, and sort order
10. WHEN documentation is generated, THE Doc_Generator SHALL present transformations in dependency order from source dataframes to final output dataframes
11. WHEN documentation is generated for a derived dataframe containing LITERAL expressions, THE Doc_Generator SHALL identify and document the constant value being assigned (e.g., LITERAL(TERMINATED) produces a constant column with value "TERMINATED")

### Requirement 8: Documentation Generation - Data Lineage

**User Story:** As a user, I want the generated documentation to show data lineage, so that I can trace data flow from source to target.

#### Acceptance Criteria

1. WHEN documentation is generated, THE Doc_Generator SHALL produce a Data Lineage section showing the directed flow from source dataframes through derived dataframes to output dataframes
2. WHEN documentation is generated, THE Doc_Generator SHALL resolve the complete dependency chain for each output dataframe back to its originating source dataframes
3. WHEN multiple Pipeline_JSON files are uploaded, THE Doc_Generator SHALL show cross-pipeline dependencies if output targets of one pipeline match source tables of another

### Requirement 9: Documentation Generation - Output Targets

**User Story:** As a user, I want the generated documentation to describe all output targets, so that I can understand where processed data is written.

#### Acceptance Criteria

1. WHEN documentation is generated, THE Doc_Generator SHALL produce an Outputs section listing each output dataframe by its ID
2. WHEN documentation is generated for a JDBC-type output, THE Doc_Generator SHALL display the target table name, format, write mode, and batchsize for the output
3. WHEN documentation is generated for a Parquet-type output, THE Doc_Generator SHALL display the target file path from tgtDFsOptionsMP, the format as "parquet", and the write mode
4. WHEN documentation is generated, THE Doc_Generator SHALL display the target connection for each output
5. WHEN a single derived dataframe is written to multiple output targets, THE Doc_Generator SHALL list each output target separately and indicate that the same dataframe feeds multiple destinations

### Requirement 10: Execution Sequence via Properties Files

**User Story:** As a user, I want to define execution sequence of pipeline files per domain folder using properties files, so that the documentation reflects the correct order of pipeline execution.

#### Acceptance Criteria

1. THE App SHALL read a properties file from each Domain_Folder to determine the execution sequence of Pipeline_JSON files within that folder
2. WHEN a properties file is present for a domain folder, THE Doc_Generator SHALL order the pipeline documentation according to the sequence defined in the properties file
3. WHEN multiple files from the same domain folder are uploaded, THE Doc_Generator SHALL display the execution order number alongside each pipeline in the generated documentation
4. IF a properties file is missing for a domain folder containing uploaded files, THEN THE App SHALL display a warning and default to alphabetical ordering of files within that folder

### Requirement 11: Multi-File Documentation

**User Story:** As a user, I want to upload multiple Pipeline_JSON files and receive a consolidated documentation output, so that I can understand the full scope of pipelines across domains.

#### Acceptance Criteria

1. WHEN multiple Pipeline_JSON files are uploaded, THE Doc_Generator SHALL produce a consolidated document with a table of contents listing all pipelines grouped by domain folder
2. WHEN multiple Pipeline_JSON files are uploaded, THE Doc_Generator SHALL generate individual pipeline documentation sections for each file
3. WHEN multiple Pipeline_JSON files are uploaded, THE Doc_Generator SHALL produce a summary section showing total source connections, total transformations, and total output targets across all pipelines

### Requirement 12: Documentation Output Format

**User Story:** As a user, I want the generated documentation in a readable format, so that I can share it with stakeholders and use it for reference.

#### Acceptance Criteria

1. THE Doc_Generator SHALL produce documentation in Markdown format
2. THE Doc_Generator SHALL use hierarchical headings, tables, and code blocks to structure the documentation for readability
3. WHEN documentation is generated, THE App SHALL provide a download option for the generated Markdown file
4. WHEN documentation is generated, THE App SHALL display a rendered preview of the Markdown documentation within the application interface

### Requirement 13: Documentation Generation - Variable Resolution

**User Story:** As a user, I want the generated documentation to show which job-level variables are referenced in queries and expressions, so that I can understand the runtime parameterization of the pipeline.

#### Acceptance Criteria

1. WHEN documentation is generated, THE Doc_Generator SHALL produce a Variable Usage section listing each variable defined in variablesMP along with its default value
2. WHEN documentation is generated, THE Doc_Generator SHALL identify all occurrences of `${variable_name}` syntax within source queries (srcDFsOptionsMP query values) and document which variables each source query references
3. WHEN documentation is generated, THE Doc_Generator SHALL identify all occurrences of `${variable_name}` syntax within derived dataframe expressions (derivedDfPipelineMappingMP) and document which variables each transformation references
4. WHEN documentation is generated, THE Doc_Generator SHALL produce a cross-reference table mapping each variable to the list of source dataframes and derived dataframes that use the variable
5. IF a `${variable_name}` reference in a query or expression does not match any variable defined in variablesMP, THEN THE Doc_Generator SHALL flag the unresolved variable reference as a warning in the documentation

### Requirement 14: Scalable Documentation for Large Pipelines

**User Story:** As a user working with complex pipelines (70+ steps, 80-100 columns per step), I want the documentation to remain readable by showing core logic flow rather than listing every column, so that I can quickly understand the pipeline without information overload.

#### Acceptance Criteria

1. WHEN a pipeline has 10 or more derived dataframes OR any dataframe has 15 or more column mappings, THE Doc_Generator SHALL switch to compressed documentation mode
2. IN compressed mode, THE Doc_Generator SHALL display a Pipeline Flow summary table showing step number, dataframe name, type, sources, column count, and key logic description for each transformation
3. IN compressed mode, THE Doc_Generator SHALL only expand full detail for transformations that contain filters, join conditions, or aggregations
4. IN compressed mode, THE Doc_Generator SHALL summarize source queries by truncating to 80 characters with an ellipsis indicator
5. IN compressed mode, THE Doc_Generator SHALL still include all sections (Job Overview, Connections, Sources, Lineage, Outputs, Variable Usage, Recommendations)

### Requirement 15: Pipeline Optimization Recommendations

**User Story:** As a user, I want the documentation to suggest how the pipeline logic can be compressed or improved, so that I can identify redundancies and simplify my data transformations.

#### Acceptance Criteria

1. WHEN documentation is generated, THE Doc_Generator SHALL produce an Optimization Recommendations section at the end of each pipeline document
2. THE Analyzer SHALL detect duplicate join conditions reused across multiple dataframes and report them as consolidation opportunities
3. THE Analyzer SHALL detect duplicate column mapping patterns across map-type dataframes and report them as merge candidates
4. THE Analyzer SHALL detect pass-through map dataframes (no filter, only column forwarding, single consumer) and recommend inlining them into the consuming dataframe
5. THE Analyzer SHALL detect multiple joins between the same source pair and recommend consolidating them into a single join
6. THE Analyzer SHALL detect chained map dataframes without filters and recommend merging the column selections into a single step
7. THE Analyzer SHALL detect unused source dataframes (defined but never referenced) and flag them as warnings
8. THE Analyzer SHALL compute a complexity score for each pipeline based on the number of steps, columns, and join conditions
9. IF no optimization opportunities are detected, THE Doc_Generator SHALL display a message indicating the pipeline structure is clean
