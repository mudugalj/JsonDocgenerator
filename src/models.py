"""Core data models for the JSON Doc Generator."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class SourceType(Enum):
    JDBC = "jdbc"
    HIVE = "hive"
    PARQUET = "parquet"


class TransformationType(Enum):
    MAP = "map"
    JOIN = "join"
    UNION = "union"
    AGG = "agg"


class JoinType(Enum):
    INNER = "inner"
    LEFT = "left"
    UNION = "union"


class DatabaseType(Enum):
    ORACLE = "oracle"
    MYSQL = "mysql"
    HIVE = "hive"
    PARQUET = "parquet"


class OutputFormat(Enum):
    JDBC = "jdbc"
    PARQUET = "parquet"


@dataclass
class Variable:
    key: str
    value: str | int | float


@dataclass
class JobInfo:
    job_id: str
    variables: list[Variable] = field(default_factory=list)


@dataclass
class Connection:
    id: int
    driver: str
    url: str
    user: str
    password: str
    database_type: DatabaseType

    @staticmethod
    def derive_database_type(driver: str) -> DatabaseType:
        driver_lower = driver.lower()
        if "oracle" in driver_lower:
            return DatabaseType.ORACLE
        if "mysql" in driver_lower:
            return DatabaseType.MYSQL
        if "hive" in driver_lower:
            return DatabaseType.HIVE
        return DatabaseType.ORACLE  # default fallback


@dataclass
class ColumnMapping:
    """Represents a single column mapping entry."""
    source_df: Optional[str] = None
    source_column: Optional[str] = None
    alias: Optional[str] = None
    expression: Optional[str] = None
    is_literal: bool = False
    literal_value: Optional[str] = None
    raw: str = ""  # original raw string
    is_descending: bool = False


@dataclass
class SourceDataframe:
    id: str
    source_type: SourceType
    connection_id: int
    source_options_id: int
    query: Optional[str] = None
    dbtable: Optional[str] = None
    path: Optional[str] = None
    source_filter: Optional[str] = None


@dataclass
class DerivedDataframe:
    id: str
    transformation_type: TransformationType
    # For map type
    source: Optional[str] = None
    columns: list[ColumnMapping] = field(default_factory=list)
    src_filter: list[str] = field(default_factory=list)
    # For join type
    join_type: Optional[JoinType] = None
    source_a: Optional[str] = None
    source_b: Optional[str] = None
    join_expressions: list[str] = field(default_factory=list)
    join_expressions_or: list[str] = field(default_factory=list)
    src_a_filter: list[str] = field(default_factory=list)
    src_b_filter: list[str] = field(default_factory=list)
    # For union type
    source_a_columns: list[ColumnMapping] = field(default_factory=list)
    source_b_columns: list[ColumnMapping] = field(default_factory=list)
    # For agg type
    group_by: list[str] = field(default_factory=list)
    aggregations: list[str] = field(default_factory=list)
    sort: list[str] = field(default_factory=list)


@dataclass
class OutputTarget:
    dataframe_id: str
    output_format: OutputFormat
    connection_id: int
    target_id: int
    table_name: Optional[str] = None
    path: Optional[str] = None
    mode: Optional[str] = None
    batchsize: Optional[int] = None


@dataclass
class PipelineModel:
    """Complete internal representation of a single Pipeline_JSON file."""
    filename: str
    domain: Optional[str] = None
    job: Optional[JobInfo] = None
    connections: list[Connection] = field(default_factory=list)
    sources: list[SourceDataframe] = field(default_factory=list)
    derived: list[DerivedDataframe] = field(default_factory=list)
    outputs: list[OutputTarget] = field(default_factory=list)


# --- Lineage Models ---


@dataclass
class LineageNode:
    id: str
    node_type: str  # "source", "derived", "output"
    label: str


@dataclass
class LineageEdge:
    from_id: str
    to_id: str
    relationship: str  # "feeds", "joins_with", "unions_with"


@dataclass
class LineageDAG:
    nodes: list[LineageNode] = field(default_factory=list)
    edges: list[LineageEdge] = field(default_factory=list)

    def topological_order(self) -> list[str]:
        """Returns node IDs in dependency order (sources first)."""
        in_degree: dict[str, int] = {n.id: 0 for n in self.nodes}
        adjacency: dict[str, list[str]] = {n.id: [] for n in self.nodes}
        for edge in self.edges:
            if edge.to_id in in_degree:
                in_degree[edge.to_id] += 1
            if edge.from_id in adjacency:
                adjacency[edge.from_id].append(edge.to_id)

        queue = [nid for nid, deg in in_degree.items() if deg == 0]
        result: list[str] = []
        while queue:
            queue.sort()
            node = queue.pop(0)
            result.append(node)
            for neighbor in adjacency.get(node, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(result) != len(self.nodes):
            raise ValueError("Circular dependency detected in pipeline DAG")
        return result

    def trace_to_sources(self, output_id: str) -> list[list[str]]:
        """Returns all paths from an output back to source nodes."""
        reverse_adj: dict[str, list[str]] = {n.id: [] for n in self.nodes}
        for edge in self.edges:
            if edge.to_id in reverse_adj:
                reverse_adj[edge.to_id].append(edge.from_id)

        source_ids = {n.id for n in self.nodes if n.node_type == "source"}
        paths: list[list[str]] = []

        def dfs(current: str, path: list[str]) -> None:
            if current in source_ids:
                paths.append(list(reversed(path)))
                return
            for parent in reverse_adj.get(current, []):
                if parent not in path:
                    dfs(parent, path + [parent])

        dfs(output_id, [output_id])
        return paths


@dataclass
class CrossPipelineLink:
    source_pipeline: str
    source_output_table: str
    target_pipeline: str
    target_source_query: str
