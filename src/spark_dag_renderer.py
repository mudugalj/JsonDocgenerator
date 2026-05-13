"""Spark DAG Mermaid rendering and Markdown report generation."""

from src.spark_dag_models import SparkDAG, SparkOperation
from src.spark_dag_traversal import level_order, topological_order

# Node shapes per operation type
SHAPES = {
    SparkOperation.READ: ("[", "]"),        # rectangle
    SparkOperation.SELECT: ("(", ")"),      # rounded
    SparkOperation.FILTER: ("(", ")"),      # rounded
    SparkOperation.JOIN: ("{", "}"),         # diamond
    SparkOperation.UNION: ("([", "])"),      # stadium
    SparkOperation.GROUP_BY_AGG: ("[[", "]]"),  # subroutine
    SparkOperation.SORT: ("(", ")"),        # rounded
    SparkOperation.WRITE: ("{{", "}}"),     # hexagon
}


def render_mermaid(dag: SparkDAG) -> str:
    """Generate Mermaid flowchart syntax from a SparkDAG."""
    lines = ["```mermaid", "graph TD"]

    for node in dag.nodes:
        shape_open, shape_close = SHAPES.get(node.operation, ("[", "]"))
        label = f"{node.operation.value}\\n{node.source_df_name or node.id}"
        safe_id = node.id.replace(":", "_").replace(".", "_")
        lines.append(f"    {safe_id}{shape_open}\"{label}\"{shape_close}")

    for edge in dag.edges:
        from_id = edge.from_node_id.replace(":", "_").replace(".", "_")
        to_id = edge.to_node_id.replace(":", "_").replace(".", "_")
        label = edge.edge_type.value
        lines.append(f"    {from_id} -->|{label}| {to_id}")

    lines.append("```")
    return "\n".join(lines)


def render_markdown_report(dag: SparkDAG) -> str:
    """Generate complete Markdown document with Mermaid diagram, traversal order, and node details."""
    lines = [f"# Spark DAG: {dag.pipeline_name}\n"]

    # Mermaid diagram
    lines.append("## Execution Flow\n")
    lines.append(render_mermaid(dag))
    lines.append("")

    # Topological execution order
    topo = topological_order(dag)
    lines.append("## Execution Order\n")
    lines.append("Nodes listed in the order they should be executed (sources first, outputs last):\n")
    for i, node_id in enumerate(topo, 1):
        node = dag.get_node(node_id)
        op = node.operation.value if node else "?"
        df_name = node.source_df_name if node else ""
        lines.append(f"{i}. `{node_id}` — {op} ({df_name})")
    lines.append("")

    # Node details table
    lines.append("## Node Details\n")
    lines.append("| # | Node ID | Operation | DataFrame | Key Info |")
    lines.append("|---|---------|-----------|-----------|----------|")
    for i, node_id in enumerate(topo, 1):
        node = dag.get_node(node_id)
        if not node:
            continue
        key_info = _get_key_info(node)
        lines.append(f"| {i} | `{node.id}` | {node.operation.value} | {node.source_df_name or ''} | {key_info} |")
    lines.append("")

    # Level-based grouping
    levels = level_order(dag)
    lines.append("## Parallel Execution Levels\n")
    lines.append("Nodes at the same level can potentially execute in parallel:\n")
    for lvl, node_ids in enumerate(levels):
        nodes_str = ", ".join(f"`{nid}`" for nid in node_ids)
        lines.append(f"**Level {lvl}:** {nodes_str}")
    lines.append("")

    return "\n".join(lines)


def _get_key_info(node) -> str:
    """Extract key info summary from a node's metadata."""
    meta = node.metadata
    op = node.operation

    if op == SparkOperation.READ:
        return f"format={meta.format}, conn={meta.connection_id}"
    elif op == SparkOperation.SELECT:
        return f"{len(meta.columns)} columns"
    elif op == SparkOperation.FILTER:
        return f"{len(meta.conditions)} condition(s)"
    elif op == SparkOperation.JOIN:
        return f"{meta.join_type} join, {len(meta.conditions)} condition(s)"
    elif op == SparkOperation.UNION:
        return "UNION ALL"
    elif op == SparkOperation.GROUP_BY_AGG:
        return f"groupBy {len(meta.group_by_columns)} cols, {len(meta.aggregations)} aggs"
    elif op == SparkOperation.SORT:
        return f"{len(meta.columns)} sort col(s)"
    elif op == SparkOperation.WRITE:
        target = meta.table_name or meta.path or ""
        return f"format={meta.format}, target={target}"
    return ""
