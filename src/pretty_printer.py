"""Pretty printer: serialize PipelineModel back to Pipeline_JSON dict."""

from src.models import (
    DerivedDataframe,
    OutputFormat,
    PipelineModel,
    SourceType,
    TransformationType,
)


def print_pipeline(model: PipelineModel) -> dict:
    """Serialize a PipelineModel back to a valid Pipeline_JSON dict."""
    result: dict = {}

    # variablesMP
    if model.job:
        result["variablesMP"] = [{
            "ID": model.job.job_id,
            "Options": [
                {"key": v.key, "value": v.value} for v in model.job.variables
            ],
        }]

    # connectionDetailsMP
    if model.connections:
        result["connectionDetailsMP"] = []
        for conn in model.connections:
            result["connectionDetailsMP"].append({
                "ID": conn.id,
                "Options": [
                    {"key": "driver", "value": conn.driver},
                    {"key": "url", "value": conn.url},
                    {"key": "user", "value": conn.user},
                    {"key": "password", "value": conn.password},
                ],
            })

    # srcDFsOptionsMP
    if model.sources:
        result["srcDFsOptionsMP"] = []
        for src in model.sources:
            opts = []
            if src.query is not None:
                opts.append({"key": "query", "value": src.query})
            if src.dbtable is not None:
                opts.append({"key": "dbtable", "value": src.dbtable})
            if src.path is not None:
                opts.append({"key": "path", "value": src.path})
            if src.source_filter is not None:
                opts.append({"key": "sourceFilter", "value": src.source_filter})
            result["srcDFsOptionsMP"].append({
                "ID": f"{src.source_options_id}.OPTIONS",
                "Options": opts,
            })

    # sourceDfPipeline
    if model.sources:
        result["sourceDfPipeline"] = []
        for src in model.sources:
            result["sourceDfPipeline"].append({
                "ID": src.id,
                "Options": [
                    {"key": "type", "value": src.source_type.value},
                    {"key": "connection", "value": src.connection_id},
                    {"key": "source", "value": src.source_options_id},
                ],
            })

    # derivedDfPipelineMappingMP
    mapping_entries = []
    for df in model.derived:
        _build_mapping_entries(df, mapping_entries)
    if mapping_entries:
        result["derivedDfPipelineMappingMP"] = mapping_entries

    # derivedDfPipelineMapSrcMap
    map_src_entries = []
    for df in model.derived:
        if df.source and df.transformation_type in (
            TransformationType.MAP, TransformationType.AGG
        ):
            map_src_entries.append({
                "ID": df.id,
                "Options": [{"key": "source", "value": df.source}],
            })
    if map_src_entries:
        result["derivedDfPipelineMapSrcMap"] = map_src_entries

    # derivedDfPipelineJoiSrcMap
    join_src_entries = []
    for df in model.derived:
        if df.transformation_type in (
            TransformationType.JOIN, TransformationType.UNION
        ) and df.source_a:
            join_type = df.join_type.value if df.join_type else "inner"
            join_src_entries.append({
                "ID": df.id,
                "Options": [
                    {"key": "joinType", "value": join_type},
                    {"key": "sourceA", "value": df.source_a},
                    {"key": "sourceB", "value": df.source_b},
                ],
            })
    if join_src_entries:
        result["derivedDfPipelineJoiSrcMap"] = join_src_entries

    # derivedDfPipeline
    if model.derived:
        result["derivedDfPipeline"] = []
        for df in model.derived:
            result["derivedDfPipeline"].append({
                "ID": df.id,
                "Options": [
                    {"key": "type", "value": df.transformation_type.value}
                ],
            })

    # outputDfPipeline
    if model.outputs:
        result["outputDfPipeline"] = []
        for out in model.outputs:
            result["outputDfPipeline"].append({
                "ID": out.dataframe_id,
                "Options": [
                    {"key": "type", "value": out.output_format.value},
                    {"key": "target", "value": out.target_id},
                    {"key": "connection", "value": out.connection_id},
                ],
            })

    # tgtDFsMP
    tgt_entries = []
    for out in model.outputs:
        tgt_entries.append({
            "ID": f"{out.target_id}.FORMAT",
            "Options": [{"key": out.output_format.value}],
        })
        if out.table_name:
            tgt_entries.append({
                "ID": f"{out.target_id}.TABLE",
                "Options": [{"key": out.table_name}],
            })
    if tgt_entries:
        result["tgtDFsMP"] = tgt_entries

    # tgtDFsOptionsMP
    tgt_opts_entries = []
    for out in model.outputs:
        opts = []
        if out.path is not None:
            opts.append({"key": "path", "value": out.path})
        if out.batchsize is not None:
            opts.append({"key": "batchsize", "value": out.batchsize})
        if out.mode is not None:
            opts.append({"key": "mode", "value": out.mode})
        if opts:
            tgt_opts_entries.append({
                "ID": f"{out.target_id}.OPTIONS",
                "Options": opts,
            })
    if tgt_opts_entries:
        result["tgtDFsOptionsMP"] = tgt_opts_entries

    return result


def _build_mapping_entries(df: DerivedDataframe, entries: list) -> None:
    """Build derivedDfPipelineMappingMP entries for a single derived DF."""
    # Main column mappings
    if df.columns:
        entries.append({
            "ID": df.id,
            "Options": [{"key": cm.raw} for cm in df.columns],
        })

    # srcFilter
    if df.src_filter:
        entries.append({
            "ID": f"{df.id}.srcFilter",
            "Options": [{"key": f} for f in df.src_filter],
        })

    # srcAFilter
    if df.src_a_filter:
        entries.append({
            "ID": f"{df.id}.srcAFilter",
            "Options": [{"key": f} for f in df.src_a_filter],
        })

    # srcBFilter
    if df.src_b_filter:
        entries.append({
            "ID": f"{df.id}.srcBFilter",
            "Options": [{"key": f} for f in df.src_b_filter],
        })

    # joinExpression
    if df.join_expressions:
        entries.append({
            "ID": f"{df.id}.joinExpression",
            "Options": [{"key": e} for e in df.join_expressions],
        })

    # joinExpressionOR
    if df.join_expressions_or:
        entries.append({
            "ID": f"{df.id}.joinExpressionOR",
            "Options": [{"key": e} for e in df.join_expressions_or],
        })

    # groupBy
    if df.group_by:
        entries.append({
            "ID": f"{df.id}.groupBy",
            "Options": [{"key": g} for g in df.group_by],
        })

    # agg
    if df.aggregations:
        entries.append({
            "ID": f"{df.id}.agg",
            "Options": [{"key": a} for a in df.aggregations],
        })

    # sort
    if df.sort:
        entries.append({
            "ID": f"{df.id}.sort",
            "Options": [{"key": s} for s in df.sort],
        })

    # Union sourceA/sourceB columns
    if df.source_a_columns:
        entries.append({
            "ID": f"{df.id}.sourceA",
            "Options": [{"key": cm.raw} for cm in df.source_a_columns],
        })

    if df.source_b_columns:
        entries.append({
            "ID": f"{df.id}.sourceB",
            "Options": [{"key": cm.raw} for cm in df.source_b_columns],
        })
