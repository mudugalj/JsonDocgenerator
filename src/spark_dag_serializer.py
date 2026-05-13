"""Spark DAG JSON serialization and deserialization."""

import dataclasses
from src.spark_dag_models import (
    EdgeType, FilterMetadata, GroupByAggMetadata, JoinMetadata,
    ReadMetadata, SelectMetadata, SortMetadata, SparkDAG, SparkEdge,
    SparkNode, SparkOperation, UnionMetadata, WriteMetadata,
)
from src.spark_dag_traversal import level_order, topological_order


def serialize_dag(dag: SparkDAG) -> dict:
    """Serialize SparkDAG to a JSON-compatible dict."""
    topo = topological_order(dag)
    levels = level_order(dag)

    nodes = []
    for node in dag.nodes:
        nodes.append({
            "id": node.id,
            "operation": node.operation.value,
            "source_df_name": node.source_df_name,
            "metadata": dataclasses.asdict(node.metadata),
        })

    edges = []
    for edge in dag.edges:
        edges.append({
            "from_node_id": edge.from_node_id,
            "to_node_id": edge.to_node_id,
            "edge_type": edge.edge_type.value,
        })

    levels_dict = {str(i): lvl for i, lvl in enumerate(levels)}

    return {
        "pipeline_name": dag.pipeline_name,
        "nodes": nodes,
        "edges": edges,
        "execution_order": topo,
        "levels": levels_dict,
    }


METADATA_MAP = {
    "SparkRead": ReadMetadata,
    "SparkSelect": SelectMetadata,
    "SparkFilter": FilterMetadata,
    "SparkJoin": JoinMetadata,
    "SparkUnion": UnionMetadata,
    "SparkGroupByAgg": GroupByAggMetadata,
    "SparkSort": SortMetadata,
    "SparkWrite": WriteMetadata,
}


def deserialize_dag(data: dict) -> SparkDAG:
    """Reconstruct a SparkDAG from a JSON-compatible dict."""
    if "pipeline_name" not in data:
        raise ValueError("Missing 'pipeline_name' in DAG JSON")
    if "nodes" not in data:
        raise ValueError("Missing 'nodes' in DAG JSON")
    if "edges" not in data:
        raise ValueError("Missing 'edges' in DAG JSON")

    nodes = []
    for n in data["nodes"]:
        op = SparkOperation(n["operation"])
        meta_cls = METADATA_MAP.get(n["operation"])
        if not meta_cls:
            raise ValueError(f"Unknown operation: {n['operation']}")
        metadata = meta_cls(**n["metadata"])
        nodes.append(SparkNode(
            id=n["id"],
            operation=op,
            metadata=metadata,
            source_df_name=n.get("source_df_name"),
        ))

    edges = []
    for e in data["edges"]:
        edges.append(SparkEdge(
            from_node_id=e["from_node_id"],
            to_node_id=e["to_node_id"],
            edge_type=EdgeType(e["edge_type"]),
        ))

    return SparkDAG(
        pipeline_name=data["pipeline_name"],
        nodes=nodes,
        edges=edges,
    )
