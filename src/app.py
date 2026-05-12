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


if __name__ == "__main__":
    app.run(debug=True, port=5000)
