"""Documentation generator: render PipelineModels as Markdown."""

from collections import defaultdict
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from src.lineage import resolve_cross_pipeline_lineage, resolve_lineage
from src.models import PipelineModel
from src.variable_resolver import find_unresolved_variables, find_variable_references

TEMPLATES_DIR = Path(__file__).parent / "templates"


def _get_env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(TEMPLATES_DIR)),
        trim_blocks=True,
        lstrip_blocks=True,
    )


def _get_multi_output_dfs(model: PipelineModel) -> list[str]:
    """Find dataframes that write to multiple output targets."""
    df_counts: dict[str, int] = {}
    for out in model.outputs:
        df_counts[out.dataframe_id] = df_counts.get(out.dataframe_id, 0) + 1
    return [df_id for df_id, count in df_counts.items() if count > 1]


def generate_single_pipeline(
    model: PipelineModel, order_num: int | None = None
) -> str:
    """Generate Markdown documentation for a single pipeline."""
    env = _get_env()
    template = env.get_template("pipeline.md.j2")

    # Resolve lineage
    dag = resolve_lineage(model)
    topo_order = dag.topological_order()

    # Order derived dataframes by topological sort
    derived_order = {nid: i for i, nid in enumerate(topo_order)}
    ordered_derived = sorted(
        model.derived,
        key=lambda d: derived_order.get(d.id, 999),
    )

    # Get lineage paths for each output
    lineage_paths: list[list[str]] = []
    for out in model.outputs:
        out_node_id = f"output:{out.target_id}"
        paths = dag.trace_to_sources(out_node_id)
        for path in paths:
            lineage_paths.append(path)

    # Variable references
    var_refs = find_variable_references(model)
    unresolved = find_unresolved_variables(model)
    var_defaults: dict[str, str] = {}
    if model.job:
        for v in model.job.variables:
            var_defaults[v.key] = str(v.value)

    # Multi-output detection
    multi_output_dfs = _get_multi_output_dfs(model)

    return template.render(
        pipeline=model,
        order_num=order_num,
        ordered_derived=ordered_derived,
        lineage_paths=lineage_paths,
        var_references=var_refs,
        var_defaults=var_defaults,
        unresolved_vars=unresolved,
        multi_output_dfs=multi_output_dfs,
    )


def generate_documentation(
    models: list[PipelineModel],
    sequence: dict[str, list[str]] | None = None,
) -> str:
    """Generate consolidated Markdown documentation for multiple pipelines.

    Args:
        models: List of parsed PipelineModels.
        sequence: Dict mapping domain folder -> ordered list of filenames.
                  If None or empty for a domain, alphabetical order is used.
    """
    if not models:
        return "# No pipelines provided\n"

    if len(models) == 1:
        return f"# Pipeline Documentation\n\n{generate_single_pipeline(models[0])}"

    env = _get_env()
    template = env.get_template("consolidated.md.j2")
    sequence = sequence or {}

    # Group pipelines by domain
    grouped: dict[str, list[PipelineModel]] = defaultdict(list)
    for model in models:
        domain = model.domain or "unknown"
        grouped[domain].append(model)

    # Order within each domain
    domain_warnings: dict[str, str] = {}
    ordered_grouped: dict[str, list[PipelineModel]] = {}
    for domain, domain_models in sorted(grouped.items()):
        domain_seq = sequence.get(domain, [])
        if domain_seq:
            order_map = {f: i for i, f in enumerate(domain_seq)}
            domain_models.sort(
                key=lambda m: order_map.get(m.filename, 999)
            )
        else:
            domain_models.sort(key=lambda m: m.filename)
            if len(domain_models) > 1:
                domain_warnings[domain] = (
                    "No properties file found. Files ordered alphabetically."
                )
        ordered_grouped[domain] = domain_models

    # Generate individual pipeline sections
    pipeline_sections: list[str] = []
    order_counter = 1
    for domain, domain_models in ordered_grouped.items():
        for model in domain_models:
            section = generate_single_pipeline(model, order_num=order_counter)
            pipeline_sections.append(section)
            order_counter += 1

    # Cross-pipeline lineage
    cross_links = resolve_cross_pipeline_lineage(models)

    # Summary stats
    total_connections = sum(len(m.connections) for m in models)
    total_transformations = sum(len(m.derived) for m in models)
    total_outputs = sum(len(m.outputs) for m in models)

    return template.render(
        grouped_pipelines=ordered_grouped,
        pipeline_sections=pipeline_sections,
        cross_pipeline_links=cross_links,
        domain_warnings=domain_warnings,
        total_pipelines=len(models),
        total_connections=total_connections,
        total_transformations=total_transformations,
        total_outputs=total_outputs,
        domain_count=len(ordered_grouped),
    )
