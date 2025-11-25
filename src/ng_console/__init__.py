# Copyright 2025 vesoft-inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
from pathlib import Path

import click
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory

from nebulagraph_python.client import NebulaClient


def create_client(hosts: str, username: str, password: str) -> NebulaClient:
    """Create and verify NebulaGraph client connection"""
    try:
        client = NebulaClient(hosts, username, password)
        # Test connection
        if not client.ping():
            raise RuntimeError("Failed to connect to NebulaGraph")
        return client
    except Exception as e:
        click.echo(f"Failed to connect to NebulaGraph: {e!s}", err=True)
        sys.exit(1)


def get_complete_query(session: PromptSession) -> str:
    """Get a complete query from user input, supporting line continuation with backslash or triple quotes"""
    lines = []
    in_multiline = False

    while True:
        prompt = "" if in_multiline else ("nebula> " if not lines else "....... ")

        line = session.prompt(prompt).rstrip()

        # Handle empty line
        if not line and not in_multiline:
            if lines:  # If we have previous lines, join them
                return " ".join(lines)
            return ""

        # Check for multiline start/end
        if line.strip() == '"""':
            if not in_multiline:  # Start multiline
                in_multiline = True
                continue
            else:  # End multiline
                in_multiline = False
                return "\n".join(lines)

        # In multiline mode, add lines without checking for continuation
        if in_multiline:
            lines.append(line)
            continue

        # Normal line handling
        if line.endswith("\\"):  # Line continuation
            lines.append(line[:-1].rstrip())  # Remove the \ and trailing spaces
        else:
            lines.append(line)
            if not in_multiline:
                return " ".join(lines)


@click.command()
@click.option(
    "--hosts",
    "-h",
    default="localhost:9669",
    help='NebulaGraph hosts (e.g., "localhost:9669")',
)
@click.option("--username", "-u", default="root", help="Username for authentication")
@click.option(
    "--password", "-p", default="Nebula@123", help="Password for authentication"
)
def console(hosts: str, username: str, password: str):
    """Interactive console for NebulaGraph"""
    client = create_client(hosts, username, password)
    click.echo(f"Connected to NebulaGraph at {hosts}")

    # Setup command history
    history_file = os.path.join(str(Path.home()), ".ng_console_history")
    session = PromptSession(history=FileHistory(history_file))

    while True:
        try:
            # Get input from user with prompt
            query = get_complete_query(session)

            # Handle exit commands
            if query.lower() in ("exit", "quit", ":q"):
                break

            # Skip empty lines
            if not query.strip():
                continue

            # Execute query and handle results
            result = client.execute(query)
            result.print()

        except KeyboardInterrupt:
            continue
        except EOFError:
            break
        except Exception as e:
            click.echo(f"Error: {e!s}", err=True)

    click.echo("Goodbye!")


if __name__ == "__main__":
    console()
