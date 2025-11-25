#!/usr/bin/env python3

import datetime
import re
import sys
from pathlib import Path


def update_version(version_type="dev", custom_suffix=None):
    """
    Update version in pyproject.toml based on version type.
    
    Args:
        version_type: 'dev', 'release', 'rc', or 'custom'
        custom_suffix: Custom suffix for version (used with 'custom' type)
    """
    # Read pyproject.toml
    pyproject_path = Path("pyproject.toml")
    if not pyproject_path.exists():
        raise FileNotFoundError("pyproject.toml not found")

    content = pyproject_path.read_text()

    # Extract current version
    version_match = re.search(r'^version\s*=\s*"([^"]+)"', content, re.MULTILINE)
    if not version_match:
        raise ValueError("Could not find version in pyproject.toml")

    current_version = version_match.group(1)
    
    # Parse the base version (remove any existing suffixes)
    base_version_match = re.match(r'^(\d+\.\d+\.\d+)', current_version)
    if not base_version_match:
        raise ValueError(f"Invalid version format: {current_version}")
    
    base_version = base_version_match.group(1)

    # Generate new version based on type
    if version_type == "dev":
        date_suffix = datetime.datetime.now().strftime("%y%m%d")
        new_version = f"{base_version}.dev{date_suffix}"
    elif version_type == "release":
        # For release, we keep the current version as-is
        new_version = current_version
    elif version_type == "rc":
        # For release candidate, extract rc number from custom_suffix or default to rc1
        if custom_suffix and custom_suffix.startswith("rc"):
            new_version = f"{base_version}{custom_suffix}"
        else:
            rc_num = custom_suffix if custom_suffix else "1"
            new_version = f"{base_version}rc{rc_num}"
    elif version_type == "post":
        # For post-release, extract post number from custom_suffix or increment existing
        if custom_suffix and custom_suffix.startswith(".post"):
            new_version = f"{base_version}{custom_suffix}"
        else:
            post_num = custom_suffix if custom_suffix else "1"
            new_version = f"{base_version}.post{post_num}"
    elif version_type == "custom":
        if custom_suffix:
            new_version = f"{base_version}{custom_suffix}"
        else:
            raise ValueError("Custom suffix required for custom version type")
    else:
        raise ValueError(f"Unknown version type: {version_type}")

    # Update version in content
    updated_content = re.sub(
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
    # Parse command line arguments
    if len(sys.argv) < 2:
        # Default to dev version
        update_version("dev")
    elif len(sys.argv) == 2:
        # First argument could be version_type or custom_suffix for backward compatibility
        arg = sys.argv[1]
        if arg in ["dev", "release", "rc", "post", "custom"]:
            update_version(arg)
        else:
            # Treat as custom suffix for backward compatibility
            update_version("custom", arg)
    elif len(sys.argv) == 3:
        # version_type and custom_suffix
        version_type = sys.argv[1]
        custom_suffix = sys.argv[2]
        update_version(version_type, custom_suffix)
    else:
        print("Usage: update_version.py [version_type] [custom_suffix]")
        print("Version types: dev, release, rc, post, custom")
        sys.exit(1)
