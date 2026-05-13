"""Spark DAG Generator: transforms PipelineModel into SparkDAG."""

from src.models import (
    DerivedDataframe,
    OutputTarget,
    PipelineModel,
    SourceDataframe,
    TransformationType,
)
from src.optimizer import OptimizationResult
from src.spark_dag_models import (
    EdgeType,
    FilterMetadata,
    GroupByAggMetadata,
    JoinMetadata,
    ReadMetadata,
    SelectMetadata,
    SortMetadata,
    SparkDAG,
    SparkEdge,
    SparkNode,
    SparkOperation,
    UnionMetadata,
    WriteMetadata,
)


def _resolve_connection(model: PipelineModel, conn_id: int) -> tuple[str | None, str | None]:
    """Resolve connection URL and driver from model."""
    conn = next((c for c in model.connections if c.id == conn_id), None)
    if conn:
        return conn.url, conn.driver
    return None, None


def _column_to_dict(cm) -> dict:
    """Convert a ColumnMapping to a serializable dict."""
    return {
        "source_df": cm.source_df,
        "source_column": cm.source_column,
        "alias": cm.alias,
        "expression": cm.expression,
        "is_literal": cm.is_literal,
        "literal_value": cm.literal_value,
        "raw": cm.raw,
    }


def generate_spark_dag(model: PipelineModel) -> SparkDAG:
    """Transform a single PipelineModel into a SparkDAG."""
    dag = SparkDAG(pipeline_name=model.job.job_id if model.job else model.filename)
    nodes: list[SparkNode] = []
    edges: list[SparkEdge] = []

    # Track the "output node ID" for each logical dataframe name
    # (the node that produces the final result for that DF)
    df_output_node: dict[str, str] = {}

    # --- Source nodes (SparkRead) ---
    for src in model.sources:
        node_id = f"read:{src.id}"
        url, driver = _resolve_connection(model, src.connection_id)
        meta = ReadMetadata(
            format=src.source_type.value,
            connection_url=url,
            driver=driver,
            query=src.query,
            dbtable=src.dbtable,
            path=src.path,
            connection_id=src.connection_id,
        )
        nodes.append(SparkNode(id=node_id, operation=SparkOperation.READ, metadata=meta, source_df_name=src.id))
        df_output_node[src.id] = node_id

    # --- Derived nodes ---
    # Process in dependency order
    from src.lineage import resolve_lineage
    lineage_dag = resolve_lineage(model)
    topo = lineage_dag.topological_order()
    derived_ids = {d.id for d in model.derived}
    ordered_derived = [d for d in sorted(model.derived, key=lambda x: topo.index(x.id) if x.id in topo else 999)]

    for df in ordered_derived:
        if df.transformation_type == TransformationType.MAP:
            _generate_map_nodes(df, model, nodes, edges, df_output_node)
        elif df.transformation_type == TransformationType.JOIN:
            _generate_join_nodes(df, model, nodes, edges, df_output_node)
        elif df.transformation_type == TransformationType.UNION:
            _generate_union_nodes(df, model, nodes, edges, df_output_node)
        elif df.transformation_type == TransformationType.AGG:
            _generate_agg_nodes(df, model, nodes, edges, df_output_node)

    # --- Output nodes (SparkWrite) ---
    for out in model.outputs:
        node_id = f"write:{out.target_id}"
        url, driver = _resolve_connection(model, out.connection_id)
        meta = WriteMetadata(
            format=out.output_format.value,
            table_name=out.table_name,
            path=out.path,
            mode=out.mode,
            batchsize=out.batchsize,
            connection_url=url,
            driver=driver,
            connection_id=out.connection_id,
        )
        nodes.append(SparkNode(id=node_id, operation=SparkOperation.WRITE, metadata=meta, source_df_name=out.table_name or out.path))

        # Edge from the producing DF to the write node
        source_node = df_output_node.get(out.dataframe_id)
        if source_node:
            edges.append(SparkEdge(from_node_id=source_node, to_node_id=node_id, edge_type=EdgeType.FEEDS))

    dag.nodes = nodes
    dag.edges = edges
    return dag


def _generate_map_nodes(df: DerivedDataframe, model: PipelineModel, nodes: list, edges: list, df_output_node: dict):
    """Generate SparkFilter (if src_filter) → SparkSelect for a MAP dataframe."""
    source_node = df_output_node.get(df.source, "")
    current_input = source_node

    # If there's a filter, insert SparkFilter node
    if df.src_filter:
        filter_id = f"filter:{df.id}"
        meta = FilterMetadata(conditions=df.src_filter)
        nodes.append(SparkNode(id=filter_id, operation=SparkOperation.FILTER, metadata=meta, source_df_name=f"{df.id}_filtered"))
        edges.append(SparkEdge(from_node_id=current_input, to_node_id=filter_id, edge_type=EdgeType.FEEDS))
        current_input = filter_id

    # SparkSelect node
    select_id = f"select:{df.id}"
    columns = [_column_to_dict(cm) for cm in df.columns]
    meta = SelectMetadata(columns=columns)
    nodes.append(SparkNode(id=select_id, operation=SparkOperation.SELECT, metadata=meta, source_df_name=df.id))
    edges.append(SparkEdge(from_node_id=current_input, to_node_id=select_id, edge_type=EdgeType.FEEDS))
    df_output_node[df.id] = select_id


def _generate_join_nodes(df: DerivedDataframe, model: PipelineModel, nodes: list, edges: list, df_output_node: dict):
    """Generate SparkFilter(A/B) if needed → SparkJoin for a JOIN dataframe."""
    left_node = df_output_node.get(df.source_a, "")
    right_node = df_output_node.get(df.source_b, "")

    # Pre-join filter on source A
    if df.src_a_filter:
        filter_a_id = f"filter:{df.id}_srcA"
        meta = FilterMetadata(conditions=df.src_a_filter)
        nodes.append(SparkNode(id=filter_a_id, operation=SparkOperation.FILTER, metadata=meta, source_df_name=f"{df.source_a}_filtered"))
        edges.append(SparkEdge(from_node_id=left_node, to_node_id=filter_a_id, edge_type=EdgeType.FEEDS))
        left_node = filter_a_id

    # Pre-join filter on source B
    if df.src_b_filter:
        filter_b_id = f"filter:{df.id}_srcB"
        meta = FilterMetadata(conditions=df.src_b_filter)
        nodes.append(SparkNode(id=filter_b_id, operation=SparkOperation.FILTER, metadata=meta, source_df_name=f"{df.source_b}_filtered"))
        edges.append(SparkEdge(from_node_id=right_node, to_node_id=filter_b_id, edge_type=EdgeType.FEEDS))
        right_node = filter_b_id

    # SparkJoin node
    join_id = f"join:{df.id}"
    join_type = df.join_type.value if df.join_type else "inner"
    meta = JoinMetadata(
        join_type=join_type,
        left_input_id=left_node,
        right_input_id=right_node,
        conditions=df.join_expressions,
        conditions_or=df.join_expressions_or,
    )
    nodes.append(SparkNode(id=join_id, operation=SparkOperation.JOIN, metadata=meta, source_df_name=df.id))
    edges.append(SparkEdge(from_node_id=left_node, to_node_id=join_id, edge_type=EdgeType.JOINS_LEFT))
    edges.append(SparkEdge(from_node_id=right_node, to_node_id=join_id, edge_type=EdgeType.JOINS_RIGHT))

    # If join has column selections, add a SparkSelect after
    if df.columns:
        select_id = f"select:{df.id}"
        columns = [_column_to_dict(cm) for cm in df.columns]
        meta_sel = SelectMetadata(columns=columns)
        nodes.append(SparkNode(id=select_id, operation=SparkOperation.SELECT, metadata=meta_sel, source_df_name=df.id))
        edges.append(SparkEdge(from_node_id=join_id, to_node_id=select_id, edge_type=EdgeType.FEEDS))
        df_output_node[df.id] = select_id
    else:
        df_output_node[df.id] = join_id


def _generate_union_nodes(df: DerivedDataframe, model: PipelineModel, nodes: list, edges: list, df_output_node: dict):
    """Generate SparkUnion for a UNION dataframe."""
    left_node = df_output_node.get(df.source_a, "")
    right_node = df_output_node.get(df.source_b, "")

    union_id = f"union:{df.id}"
    left_cols = [_column_to_dict(cm) for cm in df.source_a_columns]
    right_cols = [_column_to_dict(cm) for cm in df.source_b_columns]
    meta = UnionMetadata(
        left_input_id=left_node,
        right_input_id=right_node,
        left_columns=left_cols,
        right_columns=right_cols,
    )
    nodes.append(SparkNode(id=union_id, operation=SparkOperation.UNION, metadata=meta, source_df_name=df.id))
    edges.append(SparkEdge(from_node_id=left_node, to_node_id=union_id, edge_type=EdgeType.UNIONS_LEFT))
    edges.append(SparkEdge(from_node_id=right_node, to_node_id=union_id, edge_type=EdgeType.UNIONS_RIGHT))
    df_output_node[df.id] = union_id


def _generate_agg_nodes(df: DerivedDataframe, model: PipelineModel, nodes: list, edges: list, df_output_node: dict):
    """Generate SparkGroupByAgg (→ SparkSort if sort) for an AGG dataframe."""
    source_node = df_output_node.get(df.source, "")

    # SparkGroupByAgg
    agg_id = f"agg:{df.id}"
    meta = GroupByAggMetadata(
        group_by_columns=df.group_by,
        aggregations=df.aggregations,
    )
    nodes.append(SparkNode(id=agg_id, operation=SparkOperation.GROUP_BY_AGG, metadata=meta, source_df_name=df.id))
    edges.append(SparkEdge(from_node_id=source_node, to_node_id=agg_id, edge_type=EdgeType.FEEDS))

    current_output = agg_id

    # SparkSort if sort exists
    if df.sort:
        sort_id = f"sort:{df.id}"
        sort_cols = []
        for s in df.sort:
            descending = ".desc()" in s
            col_name = s.replace(".desc()", "").strip()
            sort_cols.append({"column": col_name, "descending": descending})
        meta_sort = SortMetadata(columns=sort_cols)
        nodes.append(SparkNode(id=sort_id, operation=SparkOperation.SORT, metadata=meta_sort, source_df_name=f"{df.id}_sorted"))
        edges.append(SparkEdge(from_node_id=agg_id, to_node_id=sort_id, edge_type=EdgeType.FEEDS))
        current_output = sort_id

    df_output_node[df.id] = current_output


def generate_spark_dags(result: OptimizationResult) -> list[SparkDAG]:
    """Generate a SparkDAG for each optimized PipelineModel."""
    return [generate_spark_dag(m) for m in result.optimized_models]
