#!/usr/bin/env python3
"""Ensure every source file under ``src`` carries an Apache 2.0 header."""

from __future__ import annotations

import re
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

HEADER_SENTINEL: str = "Licensed under the Apache License, Version 2.0"
CODING_RE: re.Pattern[str] = re.compile(r"^#.*coding[:=]\s*([-\w.]+)")


@dataclass(slots=True, kw_only=True)
class HeaderConfig:
    """Bundle the knobs the script needs for deterministic header insertion."""

    root: Path
    extensions: tuple[str, ...]
    header_lines: tuple[str, ...]


def parse_args() -> Namespace:
    """Build the CLI interface so the script can be reused in CI as-is."""

    parser: ArgumentParser = ArgumentParser(
        description="Add Apache 2.0 headers to every source file under a root.",
    )
    # Describe every supported switch so the behavior remains self-documenting.
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("src"),
        help="Root directory containing source files. Defaults to ./src",
    )
    parser.add_argument(
        "--organization",
        default="vesoft-inc",
        help="Organization name that appears in the copyright line.",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=datetime.now(tz=timezone.utc).year,
        help="Year to place in the header. Defaults to the current year.",
    )
    parser.add_argument(
        "--extensions",
        default=".py,.pyi,.pyx",
        help="Comma separated list of file extensions to update.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show which files would change without writing to disk.",
    )
    return parser.parse_args()


def build_header(*, organization: str, year: int) -> tuple[str, ...]:
    """Return the canonical Apache 2.0 comment block for the given org and year."""

    # Reuse an immutable tuple so repeated insertions avoid extra allocations.
    return (
        f"# Copyright {year} {organization}",
        "#",
        '# Licensed under the Apache License, Version 2.0 (the "License");',
        "# you may not use this file except in compliance with the License.",
        "# You may obtain a copy of the License at",
        "#",
        "#     http://www.apache.org/licenses/LICENSE-2.0",
        "#",
        "# Unless required by applicable law or agreed to in writing, software",
        '# distributed under the License is distributed on an "AS IS" BASIS,',
        "# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
        "# See the License for the specific language governing permissions and",
        "# limitations under the License.",
    )


def gather_targets(config: HeaderConfig) -> list[Path]:
    """Collect and sort candidate files from the provided root."""

    targets: list[Path] = []
    for candidate in sorted(config.root.rglob("*")):
        # Skip directories early to avoid redundant system calls.
        if candidate.is_dir():
            continue
        # Ignore files outside the configured extensions list.
        if candidate.suffix not in config.extensions:
            continue
        targets.append(candidate)
    return targets


def has_header(lines: list[str]) -> bool:
    """Detect whether an Apache 2.0 header already exists within the first block."""

    search_window: list[str] = lines[: len(lines[:20])]
    # Checking for the sentinel keeps the detection resilient to spacing tweaks.
    return any(HEADER_SENTINEL in line for line in search_window)


def insertion_index(lines: list[str]) -> int:
    """Determine where to inject the header to preserve shebang & encoding lines."""

    idx: int = 0
    if idx < len(lines) and lines[idx].startswith("#!"):
        # Keep the shebang untouched at the very top of the file.
        idx += 1
    if idx < len(lines) and CODING_RE.match(lines[idx] or ""):
        # Leave python encoding declarations directly beneath the shebang.
        idx += 1
    return idx


def render_new_lines(
    *,
    original_lines: list[str],
    header_lines: tuple[str, ...],
) -> list[str]:
    """Insert header lines while ensuring there is a single blank separator."""

    insert_at: int = insertion_index(original_lines)
    rest: list[str] = original_lines[insert_at:]
    needs_blank_line: bool = not rest or bool(rest[0].strip())
    header_block: list[str] = [*header_lines]
    if needs_blank_line:
        # Append one blank line so the header is visually separated from code.
        header_block.append("")
    return [*original_lines[:insert_at], *header_block, *rest]


def apply_header_to_file(
    *,
    path: Path,
    config: HeaderConfig,
    dry_run: bool,
) -> bool:
    """Apply the header to a single file; return True when a change is required."""

    raw_text: str = path.read_text(encoding="utf-8")
    lines: list[str] = raw_text.splitlines()
    if has_header(lines):
        # Returning early keeps idempotent runs fast.
        return False
    new_lines: list[str] = render_new_lines(
        original_lines=lines,
        header_lines=config.header_lines,
    )
    if dry_run:
        # A dry run only reports the path slated for mutation.
        print(f"[DRY-RUN] Would update {path}")
        return True
    next_text: str = "\n".join(new_lines) + "\n"
    path.write_text(next_text, encoding="utf-8")
    print(f"Updated {path}")
    return True


def run(config: HeaderConfig, *, dry_run: bool) -> int:
    """Walk the tree and rewrite every file missing the required header."""

    updated: int = 0
    for target in gather_targets(config):
        # Attempt to apply headers sequentially to keep the output deterministic.
        if apply_header_to_file(path=target, config=config, dry_run=dry_run):
            updated += 1
    return updated


def parse_extensions(raw_extensions: str) -> tuple[str, ...]:
    """Normalize the extension flag into a tuple that the config can store."""

    cleaned: list[str] = []
    for raw_chunk in raw_extensions.split(","):
        token: str = raw_chunk.strip()
        if not token:
            continue
        # Ensure every token starts with a dot so comparisons stay consistent.
        cleaned.append(token if token.startswith(".") else f".{token}")
    return tuple(cleaned)


def main() -> None:
    """Entry point that wires CLI parsing together with the header runner."""

    args: Namespace = parse_args()
    config: HeaderConfig = HeaderConfig(
        root=args.root,
        extensions=parse_extensions(args.extensions),
        header_lines=build_header(organization=args.organization, year=args.year),
    )
    if not config.root.exists():
        # Guardrail for misconfigured roots to make failures actionable.
        raise SystemExit(f"Root directory {config.root} does not exist.")
    updated: int = run(config, dry_run=args.dry_run)
    message: str = "Dry run complete." if args.dry_run else "Header update complete."
    print(f"{message} Files touched: {updated}")


if __name__ == "__main__":
    main()

