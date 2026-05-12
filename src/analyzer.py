"""Pipeline analyzer: detect optimization opportunities and produce recommendations.

Handles large pipelines (70+ steps, 80-100 columns per step) by:
1. Summarizing column counts instead of listing all columns
2. Detecting duplicate/similar join conditions across dataframes
3. Detecting duplicate column mappings that could be collapsed
4. Identifying candidates for dataframe consolidation
5. Producing actionable recommendations
"""

from dataclasses import dataclass, field
from collections import defaultdict

from src.models import DerivedDataframe, PipelineModel, TransformationType


@dataclass
class DuplicateJoinPattern:
    """A join condition pattern reused across multiple dataframes."""
    pattern: str
    dataframe_ids: list[str] = field(default_factory=list)


@dataclass
class DuplicateMapPattern:
    """A set of column mappings reused across multiple map dataframes."""
    columns_signature: str  # hash/signature of the column set
    dataframe_ids: list[str] = field(default_factory=list)
    shared_columns: list[str] = field(default_factory=list)


@dataclass
class CollapseCandidate:
    """A group of dataframes that could potentially be collapsed into one."""
    reason: str
    dataframe_ids: list[str] = field(default_factory=list)
    savings_description: str = ""


@dataclass
class PipelineRecommendations:
    """Optimization recommendations for a pipeline."""
    duplicate_joins: list[DuplicateJoinPattern] = field(default_factory=list)
    duplicate_maps: list[DuplicateMapPattern] = field(default_factory=list)
    collapse_candidates: list[CollapseCandidate] = field(default_factory=list)
    general_notes: list[str] = field(default_factory=list)
    complexity_score: int = 0  # higher = more complex


# Threshold for "large" pipeline — triggers compressed output
LARGE_PIPELINE_THRESHOLD = 10  # derived dataframes
LARGE_COLUMN_THRESHOLD = 15   # columns per dataframe


def _normalize_join_expr(expr: str) -> str:
    """Normalize a join expression for comparison (strip whitespace, lowercase)."""
    return expr.strip().lower().replace(" ", "")


def _get_column_aliases(df: DerivedDataframe) -> list[str]:
    """Extract output column aliases from a dataframe's mappings."""
    aliases = []
    for cm in df.columns:
        if cm.alias:
            aliases.append(cm.alias)
    return sorted(aliases)


def _columns_signature(df: DerivedDataframe) -> str:
    """Create a signature from column source patterns (ignoring aliases)."""
    patterns = []
    for cm in df.columns:
        if cm.source_df and cm.source_column:
            patterns.append(f"{cm.source_df}.{cm.source_column}")
        elif cm.expression:
            patterns.append(cm.expression[:50])  # truncate long expressions
    return "|".join(sorted(patterns))


def analyze_pipeline(model: PipelineModel) -> PipelineRecommendations:
    """Analyze a pipeline for optimization opportunities."""
    recs = PipelineRecommendations()

    derived = model.derived
    recs.complexity_score = (
        len(derived) * 2
        + sum(len(d.columns) for d in derived)
        + sum(len(d.join_expressions) for d in derived)
    )

    # --- Detect duplicate join conditions ---
    join_pattern_map: dict[str, list[str]] = defaultdict(list)
    for df in derived:
        if df.transformation_type == TransformationType.JOIN:
            for expr in df.join_expressions:
                normalized = _normalize_join_expr(expr)
                join_pattern_map[normalized].append(df.id)

    for pattern, df_ids in join_pattern_map.items():
        if len(df_ids) > 1:
            recs.duplicate_joins.append(
                DuplicateJoinPattern(pattern=pattern, dataframe_ids=df_ids)
            )

    # --- Detect duplicate/overlapping column mappings ---
    map_dfs = [d for d in derived if d.transformation_type == TransformationType.MAP]
    sig_map: dict[str, list[str]] = defaultdict(list)
    for df in map_dfs:
        sig = _columns_signature(df)
        if sig:
            sig_map[sig].append(df.id)

    for sig, df_ids in sig_map.items():
        if len(df_ids) > 1:
            # Find the shared columns
            first_df = next(d for d in map_dfs if d.id == df_ids[0])
            shared = _get_column_aliases(first_df)
            recs.duplicate_maps.append(
                DuplicateMapPattern(
                    columns_signature=sig[:80],
                    dataframe_ids=df_ids,
                    shared_columns=shared[:10],  # cap display
                )
            )

    # --- Detect collapse candidates ---
    # Pattern 1: Sequential map→join where the map just passes through columns
    for df in derived:
        if df.transformation_type == TransformationType.MAP and df.source:
            # Check if this map is only used as input to a single join
            consumers = [
                d for d in derived
                if d.source_a == df.id or d.source_b == df.id or d.source == df.id
            ]
            if len(consumers) == 1:
                consumer = consumers[0]
                # If the map has no filter and just passes columns through
                if not df.src_filter and all(
                    cm.source_df and cm.source_column and not cm.expression
                    for cm in df.columns
                ):
                    recs.collapse_candidates.append(CollapseCandidate(
                        reason="Pass-through map with single consumer",
                        dataframe_ids=[df.id, consumer.id],
                        savings_description=(
                            f"'{df.id}' only passes columns to '{consumer.id}'. "
                            f"Consider inlining the column selection directly into "
                            f"'{consumer.id}' to reduce pipeline steps."
                        ),
                    ))

    # Pattern 2: Multiple joins on the same key from the same source
    join_dfs = [d for d in derived if d.transformation_type == TransformationType.JOIN]
    source_pair_map: dict[tuple[str, str], list[str]] = defaultdict(list)
    for df in join_dfs:
        if df.source_a and df.source_b:
            pair = (df.source_a, df.source_b)
            source_pair_map[pair].append(df.id)

    for pair, df_ids in source_pair_map.items():
        if len(df_ids) > 1:
            recs.collapse_candidates.append(CollapseCandidate(
                reason="Multiple joins between same source pair",
                dataframe_ids=df_ids,
                savings_description=(
                    f"Dataframes {df_ids} all join '{pair[0]}' with '{pair[1]}'. "
                    f"Consider consolidating into a single join and selecting "
                    f"all needed columns at once."
                ),
            ))

    # Pattern 3: Chain of maps with no filter (A→B→C where B has no logic)
    for df in map_dfs:
        if df.source and not df.src_filter:
            source_df = next((d for d in derived if d.id == df.source), None)
            if source_df and source_df.transformation_type == TransformationType.MAP:
                if not source_df.src_filter:
                    recs.collapse_candidates.append(CollapseCandidate(
                        reason="Chained maps without filters",
                        dataframe_ids=[source_df.id, df.id],
                        savings_description=(
                            f"'{source_df.id}' → '{df.id}' are sequential maps "
                            f"with no filters. Consider merging column selections "
                            f"into a single map step."
                        ),
                    ))

    # --- General notes ---
    total_columns = sum(len(d.columns) for d in derived)
    if total_columns > 200:
        recs.general_notes.append(
            f"This pipeline has {total_columns} total column mappings across "
            f"{len(derived)} steps. Consider reviewing if all columns are needed "
            f"at each stage — early column pruning reduces memory and improves "
            f"readability."
        )

    if len(join_dfs) > 5:
        recs.general_notes.append(
            f"Pipeline has {len(join_dfs)} join operations. Consider whether "
            f"some joins can be combined or if a star-schema approach with a "
            f"single enrichment join would be more efficient."
        )

    unused_sources = set(s.id for s in model.sources)
    for df in derived:
        if df.source and df.source in unused_sources:
            unused_sources.discard(df.source)
        if df.source_a and df.source_a in unused_sources:
            unused_sources.discard(df.source_a)
        if df.source_b and df.source_b in unused_sources:
            unused_sources.discard(df.source_b)
    if unused_sources:
        recs.general_notes.append(
            f"Unused source dataframes detected: {', '.join(sorted(unused_sources))}. "
            f"These are defined but never referenced in the pipeline."
        )

    return recs


def is_large_pipeline(model: PipelineModel) -> bool:
    """Check if a pipeline is large enough to warrant compressed output."""
    return (
        len(model.derived) >= LARGE_PIPELINE_THRESHOLD
        or any(len(d.columns) >= LARGE_COLUMN_THRESHOLD for d in model.derived)
    )
