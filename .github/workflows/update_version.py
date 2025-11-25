#!/usr/bin/env python3

import datetime
import re
import sys
from pathlib import Path
from typing import Optional


def update_version(version_type: str = "dev", custom_suffix: Optional[str] = None) -> str:
    """
    Update the `version` field in `pyproject.toml` for supported manual build types.

    - dev: sets suffix to `.devYYMMDD` based on today's date.
    - custom: appends `custom_suffix` verbatim to the base X.Y.Z (e.g., rc1, .post2, -alpha).
    - Other version types are no-ops and keep the current version unchanged.

    Returns the new version string that was written.
    """
    # Read pyproject.toml
    pyproject_path = Path("pyproject.toml")
    if not pyproject_path.exists():
        raise FileNotFoundError("pyproject.toml not found")

    content: str = pyproject_path.read_text()

    # Extract current version
    version_match: Optional[re.Match[str]] = re.search(r'^version\s*=\s*"([^"]+)"', content, re.MULTILINE)
    if not version_match:
        raise ValueError("Could not find version in pyproject.toml")

    current_version: str = version_match.group(1)
    
    # Parse the base version (remove any existing suffixes)
    base_version_match: Optional[re.Match[str]] = re.match(r'^(\d+\.\d+\.\d+)', current_version)
    if not base_version_match:
        raise ValueError(f"Invalid version format: {current_version}")
    
    base_version: str = base_version_match.group(1)

    # Only dev/custom builds mutate the version; all others keep current version
    if version_type == "dev":
        # Generate a date-based dev suffix like .dev250101
        date_suffix: str = datetime.datetime.now().strftime("%y%m%d")
        new_version: str = f"{base_version}.dev{date_suffix}"
    elif version_type == "custom":
        if not custom_suffix:
            raise ValueError("custom_suffix is required when version_type is 'custom'")
        new_version = f"{base_version}{custom_suffix}"
    else:
        print(f"No change: leaving version as {current_version}")
        return current_version

    # Update version in content
    updated_content: str = re.sub(
        r'^version\s*=\s*"[^"]+"',
        f'version = "{new_version}"',
        content,
        flags=re.MULTILINE,
    )

    # Write back to file
    pyproject_path.write_text(updated_content)
    print(f"Updated version from {current_version} to {new_version}")
    return new_version


if __name__ == "__main__":
    # Accept optional args for compatibility; act on 'dev' or 'custom'
    version_type_arg: str = "dev"
    if len(sys.argv) >= 2:
        version_type_arg = sys.argv[1]
    custom_suffix_arg: Optional[str] = None
    if len(sys.argv) >= 3:
        custom_suffix_arg = sys.argv[2]
    update_version(version_type_arg, custom_suffix_arg)
