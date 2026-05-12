"""Lineage resolver: build DAG from PipelineModel and resolve dependencies."""

from src.models import (
    CrossPipelineLink,
    LineageDAG,
    LineageEdge,
    LineageNode,
    PipelineModel,
    TransformationType,
)


def resolve_lineage(model: PipelineModel) -> LineageDAG:
    """Build a lineage DAG from a single PipelineModel."""
    nodes: list[LineageNode] = []
    edges: list[LineageEdge] = []

    # Add source nodes
    for src in model.sources:
        label = src.id
        if src.query:
            label = f"{src.id} (query)"
        elif src.path:
            label = f"{src.id} (parquet: {src.path})"
        nodes.append(LineageNode(id=src.id, node_type="source", label=label))

    # Add derived nodes and edges
    source_ids = {s.id for s in model.sources}
    derived_ids = {d.id for d in model.derived}

    for df in model.derived:
        nodes.append(LineageNode(
            id=df.id,
            node_type="derived",
            label=f"{df.id} ({df.transformation_type.value})",
        ))

        if df.transformation_type in (
            TransformationType.JOIN, TransformationType.UNION
        ):
            # Join/union: edges from sourceA and sourceB
            if df.source_a:
                edges.append(LineageEdge(
                    from_id=df.source_a, to_id=df.id,
                    relationship="feeds",
                ))
            if df.source_b:
                rel = "unions_with" if df.transformation_type == TransformationType.UNION else "joins_with"
                edges.append(LineageEdge(
                    from_id=df.source_b, to_id=df.id,
                    relationship=rel,
                ))
        else:
            # Map/agg: edge from source
            if df.source:
                edges.append(LineageEdge(
                    from_id=df.source, to_id=df.id,
                    relationship="feeds",
                ))

    # Add output nodes and edges
    for out in model.outputs:
        out_node_id = f"output:{out.target_id}"
        label = out.table_name or out.path or f"target_{out.target_id}"
        nodes.append(LineageNode(
            id=out_node_id, node_type="output", label=label,
        ))
        edges.append(LineageEdge(
            from_id=out.dataframe_id, to_id=out_node_id,
            relationship="feeds",
        ))

    return LineageDAG(nodes=nodes, edges=edges)


def resolve_cross_pipeline_lineage(
    models: list[PipelineModel],
) -> list[CrossPipelineLink]:
    """Find cross-pipeline dependencies where one pipeline's output
    matches another pipeline's source table."""
    links: list[CrossPipelineLink] = []

    # Collect all output tables per pipeline
    output_tables: dict[str, list[str]] = {}  # table_name -> [pipeline filenames]
    for model in models:
        for out in model.outputs:
            if out.table_name:
                if out.table_name not in output_tables:
                    output_tables[out.table_name] = []
                output_tables[out.table_name].append(model.filename)

    # Check source queries for references to output tables
    for model in models:
        for src in model.sources:
            query_text = src.query or src.dbtable or ""
            for table_name, producing_pipelines in output_tables.items():
                if table_name in query_text:
                    for producer in producing_pipelines:
                        if producer != model.filename:
                            links.append(CrossPipelineLink(
                                source_pipeline=producer,
                                source_output_table=table_name,
                                target_pipeline=model.filename,
                                target_source_query=query_text,
                            ))

    return links
