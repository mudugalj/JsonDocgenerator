"""Variable resolver: detect ${variable_name} references and build cross-reference."""

import re

from src.models import PipelineModel

VARIABLE_PATTERN = re.compile(r"\$\{(\w+)\}")


def find_variable_references(model: PipelineModel) -> dict[str, list[str]]:
    """Build a cross-reference map: variable_name -> list of dataframe IDs that use it.

    Scans source queries and derived dataframe expressions.
    """
    var_refs: dict[str, list[str]] = {}

    # Initialize with defined variables
    if model.job:
        for v in model.job.variables:
            if v.key not in var_refs:
                var_refs[v.key] = []

    # Scan source queries
    for src in model.sources:
        texts = [src.query or "", src.dbtable or "", src.source_filter or ""]
        for text in texts:
            for match in VARIABLE_PATTERN.finditer(text):
                var_name = match.group(1)
                if var_name not in var_refs:
                    var_refs[var_name] = []
                if src.id not in var_refs[var_name]:
                    var_refs[var_name].append(src.id)

    # Scan derived dataframe expressions
    for df in model.derived:
        all_exprs: list[str] = []
        for cm in df.columns:
            if cm.raw:
                all_exprs.append(cm.raw)
        all_exprs.extend(df.src_filter)
        all_exprs.extend(df.src_a_filter)
        all_exprs.extend(df.src_b_filter)
        all_exprs.extend(df.join_expressions)
        all_exprs.extend(df.join_expressions_or)
        all_exprs.extend(df.group_by)
        all_exprs.extend(df.aggregations)
        all_exprs.extend(df.sort)
        for cm in df.source_a_columns:
            if cm.raw:
                all_exprs.append(cm.raw)
        for cm in df.source_b_columns:
            if cm.raw:
                all_exprs.append(cm.raw)

        for expr in all_exprs:
            for match in VARIABLE_PATTERN.finditer(expr):
                var_name = match.group(1)
                if var_name not in var_refs:
                    var_refs[var_name] = []
                if df.id not in var_refs[var_name]:
                    var_refs[var_name].append(df.id)

    return var_refs


def find_unresolved_variables(model: PipelineModel) -> list[str]:
    """Find variable references that don't match any defined variable in variablesMP."""
    defined_vars = set()
    if model.job:
        for v in model.job.variables:
            defined_vars.add(v.key)

    all_refs = find_variable_references(model)
    unresolved = [
        var_name for var_name in all_refs
        if var_name not in defined_vars and all_refs[var_name]
    ]
    return sorted(unresolved)
