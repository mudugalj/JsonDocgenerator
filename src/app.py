"""Flask web application for JSON Doc Generator."""

import json
import os

import markdown
from flask import Flask, render_template_string, request, Response, session

from src.doc_generator import generate_documentation
from src.parser import parse_pipeline
from src.validator import validate_extension, validate_schema

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key")

# In-memory storage for generated docs (per-session in production, simplified here)
_generated_docs: dict[str, str] = {}

UPLOAD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>JSON Doc Generator</title>
    <style>
        body { font-family: system-ui, sans-serif; max-width: 800px; margin: 2rem auto; padding: 0 1rem; }
        h1 { color: #333; }
        .upload-form { border: 2px dashed #ccc; padding: 2rem; text-align: center; border-radius: 8px; }
        .upload-form:hover { border-color: #666; }
        .btn { background: #2563eb; color: white; padding: 0.5rem 1.5rem; border: none; border-radius: 4px; cursor: pointer; font-size: 1rem; }
        .btn:hover { background: #1d4ed8; }
        .error { color: #dc2626; background: #fef2f2; padding: 1rem; border-radius: 4px; margin: 1rem 0; }
        .success { color: #16a34a; background: #f0fdf4; padding: 1rem; border-radius: 4px; margin: 1rem 0; }
        .file-list { text-align: left; margin: 1rem 0; }
    </style>
</head>
<body>
    <h1>JSON Doc Generator</h1>
    <p>Upload one or more ETL pipeline JSON files to generate documentation.</p>
    <form class="upload-form" method="POST" action="/upload" enctype="multipart/form-data">
        <p>Drag and drop JSON files here or click to browse</p>
        <input type="file" name="files" multiple accept=".json" id="file-input">
        <br><br>
        <button type="submit" class="btn">Generate Documentation</button>
    </form>

    {% if errors %}
    <div class="error">
        <strong>Validation Errors:</strong>
        <ul>
        {% for err in errors %}
            <li>{{ err }}</li>
        {% endfor %}
        </ul>
    </div>
    {% endif %}

    {% if accepted_files %}
    <div class="success">
        <strong>Files accepted:</strong>
        <div class="file-list">
        <ul>
        {% for f in accepted_files %}
            <li>{{ f }}</li>
        {% endfor %}
        </ul>
        </div>
        <a href="/preview" class="btn">View Documentation</a>
        <a href="/download" class="btn">Download Markdown</a>
    </div>
    {% endif %}
</body>
</html>
"""


@app.route("/", methods=["GET"])
def index():
    return render_template_string(UPLOAD_HTML, errors=None, accepted_files=None)


@app.route("/upload", methods=["POST"])
def upload():
    files = request.files.getlist("files")
    if not files or all(f.filename == "" for f in files):
        return render_template_string(
            UPLOAD_HTML, errors=["No files were provided."], accepted_files=None
        ), 400

    errors: list[str] = []
    parsed_models = []
    accepted_files: list[str] = []

    for file in files:
        filename = file.filename or ""

        # Validate extension
        if not validate_extension(filename):
            errors.append(f"[{filename}] Invalid file extension. Expected .json")
            continue

        # Parse JSON
        try:
            content = file.read().decode("utf-8")
            data = json.loads(content)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            errors.append(f"[{filename}] Invalid JSON: {str(e)}")
            continue

        # Validate schema
        schema_errors = validate_schema(data, filename)
        if schema_errors:
            errors.extend(schema_errors)
            continue

        # Determine domain from filename path
        domain = _extract_domain(filename)

        # Parse pipeline
        model = parse_pipeline(data, filename=filename, domain=domain)
        parsed_models.append(model)
        accepted_files.append(filename)

    if errors and not parsed_models:
        return render_template_string(
            UPLOAD_HTML, errors=errors, accepted_files=None
        ), 400

    # Generate documentation
    if parsed_models:
        doc_md = generate_documentation(parsed_models)
        _generated_docs["latest"] = doc_md

    return render_template_string(
        UPLOAD_HTML, errors=errors if errors else None, accepted_files=accepted_files
    )


@app.route("/preview", methods=["GET"])
def preview():
    doc_md = _generated_docs.get("latest", "")
    if not doc_md:
        return "No documentation generated yet. Please upload files first.", 404

    html_content = markdown.markdown(doc_md, extensions=["tables", "fenced_code"])
    return f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Documentation Preview</title>
        <style>
            body {{ font-family: system-ui, sans-serif; max-width: 900px; margin: 2rem auto; padding: 0 1rem; }}
            table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; }}
            th, td {{ border: 1px solid #ddd; padding: 0.5rem; text-align: left; }}
            th {{ background: #f5f5f5; }}
            code {{ background: #f5f5f5; padding: 0.2rem 0.4rem; border-radius: 3px; }}
            pre {{ background: #f5f5f5; padding: 1rem; border-radius: 4px; overflow-x: auto; }}
            a {{ color: #2563eb; }}
        </style>
    </head>
    <body>
        <p><a href="/">← Back to Upload</a> | <a href="/download">Download Markdown</a></p>
        {html_content}
    </body>
    </html>
    """


@app.route("/download", methods=["GET"])
def download():
    doc_md = _generated_docs.get("latest", "")
    if not doc_md:
        return "No documentation generated yet. Please upload files first.", 404

    return Response(
        doc_md,
        mimetype="text/markdown",
        headers={"Content-Disposition": "attachment; filename=pipeline_documentation.md"},
    )


def _extract_domain(filename: str) -> str | None:
    """Extract domain from filename path (e.g., 'finance/ap_aging.json' -> 'finance')."""
    parts = filename.replace("\\", "/").split("/")
    if len(parts) >= 2:
        return parts[-2]
    return None


# --- Optimizer Endpoints ---

from src.optimizer import optimize, generate_system_flow_mermaid, generate_pipeline_lineage_mermaid, print_pipeline

_optimization_result = {}

OPTIMIZE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pipeline Optimizer</title>
    <style>
        body { font-family: system-ui, sans-serif; max-width: 900px; margin: 2rem auto; padding: 0 1rem; }
        h1 { color: #333; }
        .upload-form { border: 2px dashed #ccc; padding: 2rem; border-radius: 8px; }
        .btn { background: #2563eb; color: white; padding: 0.5rem 1.5rem; border: none; border-radius: 4px; cursor: pointer; font-size: 1rem; }
        .btn:hover { background: #1d4ed8; }
        textarea { width: 100%; height: 100px; margin: 0.5rem 0; font-family: monospace; }
        .error { color: #dc2626; background: #fef2f2; padding: 1rem; border-radius: 4px; margin: 1rem 0; }
        .success { color: #16a34a; background: #f0fdf4; padding: 1rem; border-radius: 4px; margin: 1rem 0; }
    </style>
</head>
<body>
    <h1>Pipeline Optimizer</h1>
    <p>Upload JSON files and optionally specify execution sequence for cross-pipeline optimization.</p>

    <form class="upload-form" method="POST" action="/optimize" enctype="multipart/form-data">
        <p><strong>Upload JSON files:</strong></p>
        <input type="file" name="files" multiple accept=".json">
        <br><br>
        <p><strong>Execution Sequence</strong> (one filename per line, for cross-pipeline optimization):</p>
        <textarea name="sequence" placeholder="finance/gl_reconciliation_complex.json&#10;finance/ap_aging_complex.json&#10;sales/commission_complex.json"></textarea>
        <br><br>
        <button type="submit" class="btn">Optimize Pipelines</button>
    </form>

    {% if errors %}
    <div class="error">
        <ul>{% for e in errors %}<li>{{ e }}</li>{% endfor %}</ul>
    </div>
    {% endif %}

    {% if result %}
    <div class="success">
        <strong>Optimization complete!</strong> {{ step_count }} optimizations applied.
        <br><br>
        <a href="/optimize/preview" class="btn">View Report</a>
        <a href="/optimize/download" class="btn">Download All</a>
    </div>
    {% endif %}
</body>
</html>
"""


@app.route("/optimize", methods=["GET", "POST"])
def optimize_endpoint():
    if request.method == "GET":
        return render_template_string(OPTIMIZE_HTML, errors=None, result=None, step_count=0)

    files = request.files.getlist("files")
    sequence_text = request.form.get("sequence", "").strip()
    sequence = [line.strip() for line in sequence_text.split("\n") if line.strip()] if sequence_text else None

    if not files or all(f.filename == "" for f in files):
        return render_template_string(OPTIMIZE_HTML, errors=["No files provided."], result=None, step_count=0), 400

    errors: list[str] = []
    parsed_models = []

    for file in files:
        filename = file.filename or ""
        if not validate_extension(filename):
            errors.append(f"[{filename}] Invalid extension")
            continue
        try:
            content = file.read().decode("utf-8")
            data = json.loads(content)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            errors.append(f"[{filename}] Invalid JSON: {str(e)}")
            continue

        schema_errors = validate_schema(data, filename)
        if schema_errors:
            errors.extend(schema_errors)
            continue

        domain = _extract_domain(filename)
        model = parse_pipeline(data, filename=filename, domain=domain)
        parsed_models.append(model)

    if errors and not parsed_models:
        return render_template_string(OPTIMIZE_HTML, errors=errors, result=None, step_count=0), 400

    # Run optimizer
    result = optimize(parsed_models, sequence=sequence)
    _optimization_result["latest"] = result

    return render_template_string(
        OPTIMIZE_HTML, errors=errors if errors else None,
        result=True, step_count=len(result.steps)
    )


@app.route("/optimize/preview", methods=["GET"])
def optimize_preview():
    result = _optimization_result.get("latest")
    if not result:
        return "No optimization result. Please upload and optimize first.", 404

    # Combine report + proof + mermaid
    mermaid = generate_system_flow_mermaid(
        result.original_models, result.optimized_models, result.cross_pipeline_merges
    )

    # Per-pipeline lineage diagrams
    lineage_diagrams = []
    for m in result.optimized_models:
        lineage_diagrams.append(f"### {m.job.job_id if m.job else m.filename}\n")
        lineage_diagrams.append(generate_pipeline_lineage_mermaid(m))
        lineage_diagrams.append("")

    full_md = "\n\n".join([
        result.report_markdown,
        "---",
        "# System Flow\n\n" + mermaid,
        "---",
        "# Pipeline Lineage (Optimized)\n\n" + "\n".join(lineage_diagrams),
        "---",
        result.proof_markdown,
    ])

    html_content = markdown.markdown(full_md, extensions=["tables", "fenced_code"])
    return f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Optimization Results</title>
        <style>
            body {{ font-family: system-ui, sans-serif; max-width: 900px; margin: 2rem auto; padding: 0 1rem; }}
            table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; }}
            th, td {{ border: 1px solid #ddd; padding: 0.5rem; text-align: left; }}
            th {{ background: #f5f5f5; }}
            code {{ background: #f5f5f5; padding: 0.2rem 0.4rem; border-radius: 3px; }}
            pre {{ background: #f5f5f5; padding: 1rem; border-radius: 4px; overflow-x: auto; }}
            a {{ color: #2563eb; }}
            hr {{ margin: 2rem 0; }}
        </style>
    </head>
    <body>
        <p><a href="/optimize">← Back to Optimizer</a> | <a href="/optimize/download">Download All</a></p>
        {html_content}
    </body>
    </html>
    """


@app.route("/optimize/download", methods=["GET"])
def optimize_download():
    import io
    import zipfile

    result = _optimization_result.get("latest")
    if not result:
        return "No optimization result.", 404

    # Create zip with all artifacts
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        # Optimized JSONs
        for m in result.optimized_models:
            serialized = print_pipeline(m)
            zf.writestr(f"optimized/{m.filename}", json.dumps(serialized, indent=2))

        # Report
        zf.writestr("optimization_report.md", result.report_markdown)

        # Proof
        zf.writestr("equivalence_proof.md", result.proof_markdown)

        # Mermaid diagram
        mermaid = generate_system_flow_mermaid(
            result.original_models, result.optimized_models, result.cross_pipeline_merges
        )
        zf.writestr("system_flow.md", f"# System Flow\n\n{mermaid}")

        # Per-pipeline lineage
        for m in result.optimized_models:
            lineage = generate_pipeline_lineage_mermaid(m)
            safe_name = m.filename.replace("/", "_").replace(".json", "")
            zf.writestr(f"lineage/{safe_name}_lineage.md", f"# {m.filename} Lineage\n\n{lineage}")

    buffer.seek(0)
    return Response(
        buffer.getvalue(),
        mimetype="application/zip",
        headers={"Content-Disposition": "attachment; filename=optimization_results.zip"},
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
