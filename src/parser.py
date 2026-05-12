"""Parser for Pipeline_JSON files into internal PipelineModel."""

import re
from typing import Optional

from src.models import (
    ColumnMapping,
    Connection,
    DatabaseType,
    DerivedDataframe,
    JobInfo,
    JoinType,
    OutputFormat,
    OutputTarget,
    PipelineModel,
    SourceDataframe,
    SourceType,
    TransformationType,
    Variable,
)

# --- Expression Parsing Patterns ---
COL_PATTERN = re.compile(
    r"^col\((?:(\w+)\.)?(\w+)\)(?:\.alias\((\w+)\))?(?:\.desc\(\))?$"
)
EXPR_PATTERN = re.compile(r"^expr\((.+)\)(?:\.alias\((\w+)\))?$")
LITERAL_PATTERN = re.compile(r"LITERAL\(([^)]+)\)")
VARIABLE_PATTERN = re.compile(r"\$\{(\w+)\}")


def parse_column_mapping(raw: str) -> ColumnMapping:
    """Parse a single column mapping expression string."""
    is_desc = raw.strip().endswith(".desc()")

    # Try col() pattern
    col_match = COL_PATTERN.match(raw.strip())
    if col_match:
        source_df = col_match.group(1)
        source_col = col_match.group(2)
        alias = col_match.group(3)
        return ColumnMapping(
            source_df=source_df,
            source_column=source_col,
            alias=alias or source_col,
            raw=raw,
            is_descending=is_desc,
        )

    # Try expr() pattern
    expr_match = EXPR_PATTERN.match(raw.strip())
    if expr_match:
        expression = expr_match.group(1)
        alias = expr_match.group(2)
        # Check for LITERAL
        lit_match = LITERAL_PATTERN.search(expression)
        is_literal = lit_match is not None
        literal_value = lit_match.group(1) if lit_match else None

        return ColumnMapping(
            alias=alias,
            expression=expression,
            is_literal=is_literal,
            literal_value=literal_value,
            raw=raw,
        )

    # Fallback: store as raw
    return ColumnMapping(raw=raw)


def _get_option_value(options: list[dict], key: str) -> Optional[str]:
    """Get a value from an Options list by key."""
    for opt in options:
        if opt.get("key") == key:
            return opt.get("value")
    return None


def _get_options_dict(options: list[dict]) -> dict:
    """Convert Options list to a dict."""
    result = {}
    for opt in options:
        k = opt.get("key", "")
        v = opt.get("value")
        if k:
            result[k] = v
    return result


def _parse_variables(data: dict) -> Optional[JobInfo]:
    """Parse variablesMP section."""
    variables_mp = data.get("variablesMP", [])
    if not variables_mp:
        return None

    entry = variables_mp[0]
    job_id = str(entry.get("ID", ""))
    variables = []
    for opt in entry.get("Options", []):
        key = opt.get("key", "")
        value = opt.get("value", "")
        variables.append(Variable(key=key, value=value))

    return JobInfo(job_id=job_id, variables=variables)


def _parse_connections(data: dict) -> list[Connection]:
    """Parse connectionDetailsMP section."""
    connections = []
    for entry in data.get("connectionDetailsMP", []):
        conn_id = int(entry.get("ID", 0))
        opts = _get_options_dict(entry.get("Options", []))
        driver = str(opts.get("driver", ""))
        url = str(opts.get("url", ""))
        user = str(opts.get("user", ""))
        password = str(opts.get("password", ""))
        db_type = Connection.derive_database_type(driver)
        connections.append(Connection(
            id=conn_id, driver=driver, url=url,
            user=user, password=password, database_type=db_type,
        ))
    return connections


def _parse_sources(data: dict) -> list[SourceDataframe]:
    """Parse sourceDfPipeline and srcDFsOptionsMP sections."""
    # Build lookup for source options by numeric ID prefix
    src_options: dict[int, dict] = {}
    for entry in data.get("srcDFsOptionsMP", []):
        raw_id = str(entry.get("ID", ""))
        # Extract numeric prefix (e.g., "200200201.OPTIONS" -> 200200201)
        numeric_id = int(raw_id.split(".")[0]) if "." in raw_id else 0
        opts = {}
        for opt in entry.get("Options", []):
            key = opt.get("key", "")
            value = opt.get("value")
            if key:
                opts[key] = value
        src_options[numeric_id] = opts

    sources = []
    for entry in data.get("sourceDfPipeline", []):
        df_id = str(entry.get("ID", ""))
        opts = _get_options_dict(entry.get("Options", []))
        raw_type = str(opts.get("type", "jdbc"))
        try:
            source_type = SourceType(raw_type)
        except ValueError:
            source_type = SourceType.JDBC

        conn_id = int(opts.get("connection", 0))
        source_ref = int(opts.get("source", 0))

        # Resolve source options
        src_opts = src_options.get(source_ref, {})
        query = src_opts.get("query")
        dbtable = src_opts.get("dbtable")
        path = src_opts.get("path")
        source_filter = src_opts.get("sourceFilter")

        sources.append(SourceDataframe(
            id=df_id,
            source_type=source_type,
            connection_id=conn_id,
            source_options_id=source_ref,
            query=query,
            dbtable=dbtable,
            path=path,
            source_filter=source_filter,
        ))
    return sources


def _parse_derived(data: dict) -> list[DerivedDataframe]:
    """Parse derivedDfPipeline and related mapping sections."""
    # Build mapping lookup: ID -> list of raw expression strings
    mapping_mp: dict[str, list[str]] = {}
    for entry in data.get("derivedDfPipelineMappingMP", []):
        entry_id = str(entry.get("ID", ""))
        exprs = []
        for opt in entry.get("Options", []):
            key = opt.get("key", "")
            if key:
                exprs.append(key)
        mapping_mp[entry_id] = exprs

    # Build source map lookup
    map_src: dict[str, str] = {}
    for entry in data.get("derivedDfPipelineMapSrcMap", []):
        entry_id = str(entry.get("ID", ""))
        opts = _get_options_dict(entry.get("Options", []))
        source = opts.get("source")
        if source:
            map_src[entry_id] = str(source)

    # Build join source map lookup
    join_src: dict[str, dict] = {}
    for entry in data.get("derivedDfPipelineJoiSrcMap", []):
        entry_id = str(entry.get("ID", ""))
        opts = _get_options_dict(entry.get("Options", []))
        join_src[entry_id] = opts

    derived_list = []
    for entry in data.get("derivedDfPipeline", []):
        df_id = str(entry.get("ID", ""))
        opts = _get_options_dict(entry.get("Options", []))
        raw_type = str(opts.get("type", "map"))
        try:
            trans_type = TransformationType(raw_type)
        except ValueError:
            trans_type = TransformationType.MAP

        df = DerivedDataframe(id=df_id, transformation_type=trans_type)

        # Resolve source from map source map
        if df_id in map_src:
            df.source = map_src[df_id]

        # Resolve join sources
        if df_id in join_src:
            join_info = join_src[df_id]
            raw_join_type = str(join_info.get("joinType", "inner"))
            try:
                df.join_type = JoinType(raw_join_type)
            except ValueError:
                df.join_type = JoinType.INNER
            df.source_a = join_info.get("sourceA")
            df.source_b = join_info.get("sourceB")

        # Parse column mappings
        if df_id in mapping_mp:
            df.columns = [parse_column_mapping(e) for e in mapping_mp[df_id]]

        # Parse filters, join expressions, group-by, agg, sort
        filter_key = f"{df_id}.srcFilter"
        if filter_key in mapping_mp:
            df.src_filter = mapping_mp[filter_key]

        src_a_filter_key = f"{df_id}.srcAFilter"
        if src_a_filter_key in mapping_mp:
            df.src_a_filter = mapping_mp[src_a_filter_key]

        src_b_filter_key = f"{df_id}.srcBFilter"
        if src_b_filter_key in mapping_mp:
            df.src_b_filter = mapping_mp[src_b_filter_key]

        join_expr_key = f"{df_id}.joinExpression"
        if join_expr_key in mapping_mp:
            df.join_expressions = mapping_mp[join_expr_key]

        join_expr_or_key = f"{df_id}.joinExpressionOR"
        if join_expr_or_key in mapping_mp:
            df.join_expressions_or = mapping_mp[join_expr_or_key]

        group_by_key = f"{df_id}.groupBy"
        if group_by_key in mapping_mp:
            df.group_by = mapping_mp[group_by_key]

        agg_key = f"{df_id}.agg"
        if agg_key in mapping_mp:
            df.aggregations = mapping_mp[agg_key]

        sort_key = f"{df_id}.sort"
        if sort_key in mapping_mp:
            df.sort = mapping_mp[sort_key]

        # Union type: sourceA/sourceB columns
        source_a_key = f"{df_id}.sourceA"
        if source_a_key in mapping_mp:
            df.source_a_columns = [
                parse_column_mapping(e) for e in mapping_mp[source_a_key]
            ]

        source_b_key = f"{df_id}.sourceB"
        if source_b_key in mapping_mp:
            df.source_b_columns = [
                parse_column_mapping(e) for e in mapping_mp[source_b_key]
            ]

        derived_list.append(df)

    return derived_list


def _parse_outputs(data: dict) -> list[OutputTarget]:
    """Parse outputDfPipeline, tgtDFsMP, and tgtDFsOptionsMP sections."""
    # Build target format/table lookup
    tgt_format: dict[int, str] = {}
    tgt_table: dict[int, str] = {}
    for entry in data.get("tgtDFsMP", []):
        raw_id = str(entry.get("ID", ""))
        # e.g., "4002001.FORMAT" or "4002001.TABLE"
        parts = raw_id.split(".")
        if len(parts) == 2:
            numeric_id = int(parts[0])
            suffix = parts[1]
            # Options has single entry with just "key" (no "value")
            for opt in entry.get("Options", []):
                key = opt.get("key", "")
                if suffix == "FORMAT" and key:
                    tgt_format[numeric_id] = key
                elif suffix == "TABLE" and key:
                    tgt_table[numeric_id] = key

    # Build target options lookup
    tgt_options: dict[int, dict] = {}
    for entry in data.get("tgtDFsOptionsMP", []):
        raw_id = str(entry.get("ID", ""))
        parts = raw_id.split(".")
        if len(parts) == 2:
            numeric_id = int(parts[0])
            opts = _get_options_dict(entry.get("Options", []))
            tgt_options[numeric_id] = opts

    outputs = []
    for entry in data.get("outputDfPipeline", []):
        df_id = str(entry.get("ID", ""))
        opts = _get_options_dict(entry.get("Options", []))
        raw_format = str(opts.get("type", "jdbc"))
        try:
            out_format = OutputFormat(raw_format)
        except ValueError:
            out_format = OutputFormat.JDBC

        target_id = int(opts.get("target", 0))
        conn_id = int(opts.get("connection", 0))

        # Resolve target details
        table_name = tgt_table.get(target_id)
        target_opts = tgt_options.get(target_id, {})
        path = target_opts.get("path")
        mode = target_opts.get("mode")
        batchsize_raw = target_opts.get("batchsize")
        batchsize = int(batchsize_raw) if batchsize_raw is not None else None

        outputs.append(OutputTarget(
            dataframe_id=df_id,
            output_format=out_format,
            connection_id=conn_id,
            target_id=target_id,
            table_name=table_name,
            path=path,
            mode=mode,
            batchsize=batchsize,
        ))
    return outputs


def parse_pipeline(data: dict, filename: str = "", domain: Optional[str] = None) -> PipelineModel:
    """Parse a complete Pipeline_JSON dict into a PipelineModel."""
    return PipelineModel(
        filename=filename,
        domain=domain,
        job=_parse_variables(data),
        connections=_parse_connections(data),
        sources=_parse_sources(data),
        derived=_parse_derived(data),
        outputs=_parse_outputs(data),
    )
