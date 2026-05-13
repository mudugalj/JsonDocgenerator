"""Spark DAG data models: typed nodes, edges, and DAG structure."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class SparkOperation(Enum):
    READ = "SparkRead"
    SELECT = "SparkSelect"
    FILTER = "SparkFilter"
    JOIN = "SparkJoin"
    UNION = "SparkUnion"
    GROUP_BY_AGG = "SparkGroupByAgg"
    SORT = "SparkSort"
    WRITE = "SparkWrite"


# --- Operation Metadata ---

@dataclass
class ReadMetadata:
    format: str  # "jdbc", "parquet", "hive"
    connection_url: Optional[str] = None
    driver: Optional[str] = None
    query: Optional[str] = None
    dbtable: Optional[str] = None
    path: Optional[str] = None
    connection_id: Optional[int] = None


@dataclass
class SelectMetadata:
    columns: list[dict] = field(default_factory=list)


@dataclass
class FilterMetadata:
    conditions: list[str] = field(default_factory=list)


@dataclass
class JoinMetadata:
    join_type: str = "inner"
    left_input_id: str = ""
    right_input_id: str = ""
    conditions: list[str] = field(default_factory=list)
    conditions_or: list[str] = field(default_factory=list)


@dataclass
class UnionMetadata:
    left_input_id: str = ""
    right_input_id: str = ""
    left_columns: list[dict] = field(default_factory=list)
    right_columns: list[dict] = field(default_factory=list)


@dataclass
class GroupByAggMetadata:
    group_by_columns: list[str] = field(default_factory=list)
    aggregations: list[str] = field(default_factory=list)


@dataclass
class SortMetadata:
    columns: list[dict] = field(default_factory=list)  # [{column, descending}]


@dataclass
class WriteMetadata:
    format: str = "jdbc"
    table_name: Optional[str] = None
    path: Optional[str] = None
    mode: Optional[str] = None
    batchsize: Optional[int] = None
    connection_url: Optional[str] = None
    driver: Optional[str] = None
    connection_id: Optional[int] = None


MetadataType = (
    ReadMetadata | SelectMetadata | FilterMetadata | JoinMetadata |
    UnionMetadata | GroupByAggMetadata | SortMetadata | WriteMetadata
)


class EdgeType(Enum):
    FEEDS = "feeds"
    JOINS_LEFT = "joins_left"
    JOINS_RIGHT = "joins_right"
    UNIONS_LEFT = "unions_left"
    UNIONS_RIGHT = "unions_right"


@dataclass
class SparkEdge:
    from_node_id: str
    to_node_id: str
    edge_type: EdgeType


@dataclass
class SparkNode:
    id: str
    operation: SparkOperation
    metadata: MetadataType
    source_df_name: Optional[str] = None


@dataclass
class SparkDAG:
    pipeline_name: str
    nodes: list[SparkNode] = field(default_factory=list)
    edges: list[SparkEdge] = field(default_factory=list)

    def get_node(self, node_id: str) -> Optional[SparkNode]:
        return next((n for n in self.nodes if n.id == node_id), None)

    def get_children(self, node_id: str) -> list[str]:
        return [e.to_node_id for e in self.edges if e.from_node_id == node_id]

    def get_parents(self, node_id: str) -> list[str]:
        return [e.from_node_id for e in self.edges if e.to_node_id == node_id]
