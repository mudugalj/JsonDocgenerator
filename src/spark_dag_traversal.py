"""Spark DAG traversal algorithms: topological, reverse, level-based."""

from src.spark_dag_models import SparkDAG, SparkOperation


def topological_order(dag: SparkDAG) -> list[str]:
    """Return node IDs in dependency order. Deterministic: alphabetical at same depth."""
    in_degree: dict[str, int] = {n.id: 0 for n in dag.nodes}
    adjacency: dict[str, list[str]] = {n.id: [] for n in dag.nodes}

    for edge in dag.edges:
        if edge.to_node_id in in_degree:
            in_degree[edge.to_node_id] += 1
        if edge.from_node_id in adjacency:
            adjacency[edge.from_node_id].append(edge.to_node_id)

    queue = sorted([nid for nid, deg in in_degree.items() if deg == 0])
    result: list[str] = []

    while queue:
        node = queue.pop(0)
        result.append(node)
        children = sorted(adjacency.get(node, []))
        for child in children:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                # Insert in sorted position
                queue.append(child)
                queue.sort()

    if len(result) != len(dag.nodes):
        visited = set(result)
        cycle_nodes = [n.id for n in dag.nodes if n.id not in visited]
        raise ValueError(f"Cycle detected involving nodes: {cycle_nodes}")

    return result


def reverse_paths(dag: SparkDAG, output_node_id: str) -> list[list[str]]:
    """Return all paths from output back to source nodes (output first)."""
    if not dag.get_node(output_node_id):
        raise KeyError(f"Node '{output_node_id}' not found in DAG")

    read_ids = {n.id for n in dag.nodes if n.operation == SparkOperation.READ}
    paths: list[list[str]] = []

    def dfs(current: str, path: list[str]):
        if current in read_ids:
            paths.append(list(path))
            return
        parents = dag.get_parents(current)
        if not parents:
            paths.append(list(path))
            return
        for parent in parents:
            if parent not in path:
                dfs(parent, path + [parent])

    dfs(output_node_id, [output_node_id])
    return paths


def level_order(dag: SparkDAG) -> list[list[str]]:
    """Group node IDs by depth level. Level 0 = source (SparkRead) nodes.
    Each node's level = longest path from any source to that node."""
    # Compute longest path from any source to each node
    levels: dict[str, int] = {}
    topo = topological_order(dag)

    # Initialize: source nodes at level 0
    for node_id in topo:
        node = dag.get_node(node_id)
        if node and node.operation == SparkOperation.READ:
            levels[node_id] = 0
        else:
            # Level = max(parent levels) + 1
            parents = dag.get_parents(node_id)
            if parents:
                max_parent_level = max(levels.get(p, 0) for p in parents)
                levels[node_id] = max_parent_level + 1
            else:
                levels[node_id] = 0

    # Group by level
    max_level = max(levels.values()) if levels else 0
    result: list[list[str]] = []
    for lvl in range(max_level + 1):
        nodes_at_level = sorted([nid for nid, l in levels.items() if l == lvl])
        result.append(nodes_at_level)

    return result
