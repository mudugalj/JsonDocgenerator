"""Convert optimized JSON to a unified CSV.

Columns: Section | Seq | Name | Type | Source | Target | Expression | Connection | Options

Section values:
  job         — job name + module + variables
  connection  — one row per connection (slug, type, url)
  source      — one row per source DataFrame
  step        — one row per pipeline expression/transform (multiple rows per step)
  output      — one row per output target
"""

COLUMNS = ["Section", "Seq", "Name", "Type", "Source", "Target",
           "Expression", "Connection", "Options"]


def empty_row():
    return {c: "" for c in COLUMNS}


def _conn_type(props):
    """Derive human-readable DB type from connection props."""
    driver = str(props.get("driver", "")).lower()
    if "mysql" in driver:
        return "mysql"
    if "postgresql" in driver or "postgres" in driver:
        return "postgres"
    if "sqlserver" in driver:
        return "sqlserver"
    return "jdbc"


def _options_note(opts):
    """Serialize write/read options as a JSON string for the Options column.

    Passwords are never written; numeric strings are kept as numbers.
    Returns empty string when there are no options.
    """
    import json as _json
    cleaned = {k: v for k, v in opts.items() if k != "password"}
    return _json.dumps(cleaned) if cleaned else ""


def convert(data):
    rows = []
    seq = [0]  # mutable counter helper

    def r(section, name="", typ="", source="", target="",
          expression="", connection="", note=""):
        seq[0] += 1
        row = empty_row()
        row["Section"] = section
        row["Seq"] = seq[0]
        row["Name"] = name
        row["Type"] = typ
        row["Source"] = source
        row["Target"] = target
        row["Expression"] = expression
        row["Connection"] = connection
        row["Options"] = note
        return row

    # ── Job ──────────────────────────────────────────────────────────────────
    job = data.get("job", {})
    rows.append(r("job", name=job.get("name", ""), typ=job.get("module", "")))
    for k, v in job.get("variables", {}).items():
        rows.append(r("job", name=k, typ="variable", expression=str(v)))

    # ── Connections ───────────────────────────────────────────────────────────
    import json as _json
    seq[0] = 0  # reset seq per section
    for slug, props in data.get("connections", {}).items():
        display_name = props.get("name", slug)
        # Store full props as JSON in Expression for lossless round-trips.
        rows.append(r("connection",
                      name=slug,
                      typ=_conn_type(props),
                      expression=_json.dumps(props),
                      note=_json.dumps({"label": display_name})))

    # ── Sources ───────────────────────────────────────────────────────────────
    seq[0] = 0
    for df_name, props in data.get("sources", {}).items():
        typ = props.get("type", "jdbc")
        read_opts = props.get("readOptions", {})
        if typ == "parquet":
            # Expression = file path; no connection for file-based sources
            expr = read_opts.get("path", "")
            conn = ""
        else:
            q = read_opts.get("query", "")
            t = read_opts.get("dbtable", "")
            conn = str(props.get("connection", ""))
            
            if q:
                # Wrap queries with alias to prevent MariaDB/MySQL JDBC 'WHERE 1=0' subquery errors
                expr = f"({q}) AS {df_name}_subq"
            else:
                expr = t
                
        rows.append(r("source",
                      name=df_name,
                      typ=typ,
                      expression=expr,
                      connection=conn))

    # ── Pipeline ─────────────────────────────────────────────────────────────
    seq[0] = 0
    step_num = 0
    for entry in data.get("pipeline", []):
        step_num += 1
        df_name = entry.get("name", "")
        df_type = entry.get("type", "")

        # Determine step type label
        if df_type == "join":
            step_label = f"join:{entry.get('joinType', 'inner')}"
        else:
            step_label = df_type

        source_ref = entry.get("source",
                                entry.get("sourceA", ""))
        source_b = entry.get("sourceB", "")
        step_note_data = {"label": f"step {step_num}"}
        if source_b:
            step_note_data["sourceB"] = source_b
        step_note = _json.dumps(step_note_data)

        # Summary row for the step
        rows.append(r("step",
                      name=df_name,
                      typ=step_label,
                      source=source_ref,
                      note=step_note))

        # Expression rows
        _expr_rows(rows, r, df_name, "column",   entry.get("columns", []))
        _expr_rows(rows, r, df_name, "filter",    entry.get("srcFilter", []))
        _expr_rows(rows, r, df_name, "filterA",   entry.get("srcAFilter", []))
        _expr_rows(rows, r, df_name, "filterB",   entry.get("srcBFilter", []))
        _expr_rows(rows, r, df_name, "join_cond",    entry.get("joinExpression",   []))
        _expr_rows(rows, r, df_name, "join_cond_or", entry.get("joinExpressionOR", []))
        _expr_rows(rows, r, df_name, "group_by",  entry.get("groupBy", []))
        _expr_rows(rows, r, df_name, "agg",       entry.get("agg", []))
        _expr_rows(rows, r, df_name, "sort",      entry.get("sort", []))
        _expr_rows(rows, r, df_name, "col_A",     entry.get("sourceAColumns", []))
        _expr_rows(rows, r, df_name, "col_B",     entry.get("sourceBColumns", []))
        if entry.get("distinct"):
            rows.append(r("step", name=df_name, typ="distinct"))

    # ── Outputs ───────────────────────────────────────────────────────────────
    seq[0] = 0
    for entry in data.get("outputs", []):
        fmt = entry.get("format", "")
        write_opts = dict(entry.get("writeOptions", {}))
        if fmt == "parquet":
            # Path lives in Target; connection is unused for file sinks
            target = write_opts.pop("path", entry.get("table", ""))
            conn = ""
        else:
            target = entry.get("table", "")
            conn = str(entry.get("connection", ""))
        rows.append(r("output",
                      name=entry.get("dataframe", ""),
                      typ=fmt,
                      source=entry.get("dataframe", ""),
                      target=target,
                      connection=conn,
                      note=_options_note(write_opts)))

    return rows


def _expr_rows(rows, r_fn, df_name, op_type, expr_list):
    for expr in expr_list:
        rows.append(r_fn("step", name=df_name, typ=op_type, expression=str(expr)))
