# NebulaGraph Python Query Templating

NebulaGraph Python provides a lightweight, safe query templating helper via `execute_py`. It lets you keep queries readable while passing Python values as placeholders that are serialized into valid GQL literals and then rendered into the final statement.

## Placeholder Syntax

- Use `{{name}}` to denote a value placeholder.
- Provide a Python `dict` with keys matching placeholder names.
- Placeholders are for values only (not identifiers, keywords, tag/edge names, or property names).

## Usage

```python
import asyncio
from nebulagraph_python.client import NebulaAsyncClient

async def main() -> None:
    async with await NebulaAsyncClient.connect(
        hosts=["127.0.0.1:9669"],
        username="root",
        password="NebulaGraph01",
    ) as client:
        query = """
        RETURN {{v1}} as v1, {{v2}} as v2, {{v3}} as v3, {{v4}} as v4
        """
        args = {
            "v1": 1,
            "v2": "alice",
            "v3": [True, False, True],
            "v4": None,
        }
        res = await client.execute_py(query, args)
        res.print()
        print(res.one().as_primitive())

asyncio.run(main())
```

## How It Works

1. Serializer → GQL literals
   - A serializer function converts Python values to valid GQL literals.
   - Supported: auto‑quoted strings, integers, floats, booleans, nulls, and lists/tuples.
2. Rendering via `minijinja`
   - The query string is rendered by `minijinja`, substituting `{{placeholder}}` with the serialized GQL literals to produce the final statement.

This two‑step approach avoids manual quoting/escaping and keeps your templates focused on values only.

Tip: Keep placeholders strictly for values. Avoid templating identifiers such as tag names, edge types, property names, or keywords.

