"""Properties file reader for execution sequencing."""

import os
from pathlib import Path

PROPERTIES_FILENAME = "sequence.properties"


def read_properties(domain_folder: str) -> list[str]:
    """Read a properties file from a domain folder to get execution sequence.

    The properties file is expected to have one filename per line,
    in execution order. Lines starting with '#' are comments.
    Empty lines are ignored.

    Returns an ordered list of filenames. Returns empty list if
    properties file is missing (caller handles fallback to alphabetical).
    """
    props_path = Path(domain_folder) / PROPERTIES_FILENAME
    if not props_path.exists():
        return []

    filenames: list[str] = []
    with open(props_path, "r") as f:
        for line in f:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                filenames.append(stripped)

    return filenames


def get_ordered_files(
    domain_folder: str, uploaded_files: list[str]
) -> tuple[list[str], bool]:
    """Get files in execution order for a domain folder.

    Returns (ordered_files, has_properties_file).
    If no properties file exists, returns alphabetically sorted files.
    """
    sequence = read_properties(domain_folder)
    if sequence:
        # Filter to only uploaded files, maintaining sequence order
        ordered = [f for f in sequence if f in uploaded_files]
        # Add any uploaded files not in the sequence at the end
        remaining = [f for f in uploaded_files if f not in sequence]
        return ordered + sorted(remaining), True

    return sorted(uploaded_files), False
