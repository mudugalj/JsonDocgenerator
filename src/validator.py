"""File extension and schema validation for Pipeline_JSON files."""

REQUIRED_SECTIONS = [
    "sourceDfPipeline",
    "derivedDfPipeline",
    "outputDfPipeline",
]

ALL_KNOWN_SECTIONS = [
    "variablesMP",
    "connectionDetailsMP",
    "srcDFsOptionsMP",
    "sourceDfPipeline",
    "derivedDfPipeline",
    "derivedDfPipelineMappingMP",
    "derivedDfPipelineMapSrcMap",
    "derivedDfPipelineJoiSrcMap",
    "outputDfPipeline",
    "tgtDFsMP",
    "tgtDFsOptionsMP",
]


def validate_extension(filename: str) -> bool:
    """Check if filename has a .json extension (case-insensitive)."""
    return filename.lower().endswith(".json")


def validate_schema(data: dict, filename: str = "") -> list[str]:
    """Validate that data conforms to Pipeline_JSON schema.

    Returns a list of error messages. Empty list means valid.
    """
    errors: list[str] = []
    prefix = f"[{filename}] " if filename else ""

    if not isinstance(data, dict):
        errors.append(f"{prefix}File content is not a JSON object")
        return errors

    # Check required sections
    for section in REQUIRED_SECTIONS:
        if section not in data:
            errors.append(f"{prefix}Missing required section: {section}")

    # Validate structure of present sections
    for section in ALL_KNOWN_SECTIONS:
        if section in data:
            value = data[section]
            if not isinstance(value, list):
                errors.append(
                    f"{prefix}Section '{section}' must be an array, "
                    f"got {type(value).__name__}"
                )
                continue
            for i, entry in enumerate(value):
                if not isinstance(entry, dict):
                    errors.append(
                        f"{prefix}Section '{section}' entry {i} "
                        f"must be an object"
                    )
                    continue
                if "ID" not in entry:
                    errors.append(
                        f"{prefix}Section '{section}' entry {i} "
                        f"missing required field 'ID'"
                    )
                if "Options" not in entry:
                    errors.append(
                        f"{prefix}Section '{section}' entry {i} "
                        f"missing required field 'Options'"
                    )
                elif not isinstance(entry.get("Options"), list):
                    errors.append(
                        f"{prefix}Section '{section}' entry {i} "
                        f"'Options' must be an array"
                    )

    return errors
