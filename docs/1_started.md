# NebulaGraph Python Client Getting Started

## Installation

```bash
pip install ng-python # not yet published
```

from source

```bash
cd python
pip install -e .
```

## Get Started

1. We could easily connect and get a query result.

```python
import asyncio
from nebulagraph_python.client import NebulaAsyncClient

async def main() -> None:
    async with await NebulaAsyncClient.connect(
        hosts=["127.0.0.1:9669"],
        username="root",
        password="NebulaGraph01",
    ) as client:
        result = await client.execute("RETURN 1 AS a, 2 AS b")
        result.print()

asyncio.run(main())
```

2. Then we could inspect the result ourselves.

```python
# Print the result in table style
result.print()
```

```
╔═══╤═══╗
║   │   ║
║ a │ b ║
║   │   ║
╟───┼───╢
║   │   ║
║ 1 │ 2 ║
║   │   ║
╚═══╧═══╝

Summary
├── Rows: 1
└── Latency: 1450μs
```

```python
# Get one row
row = result.one()

# Get one value
cell = row["a"].cast_primitive()

# Print its value
print(cell, type(cell))
```

```
1 <class 'int'>
```

3. We could actually get primitive values from the result set.

```python
print(result.as_primitive_by_column())
print(list(result.as_primitive_by_row()))
```

```
{'a': [1], 'b': [2]}
[{'a': 1, 'b': 2}]
```

4. If needed we could also get a pandas dataframe from the result.

We need to install pandas first.

```bash
pip install pandas
```

Then we could get a pandas dataframe like this:

```python
result = await client.execute(query)

df = result.as_pandas_df()
```

```
   a  b
0  1  2
```


## Synchronous Client

Prefer a blocking API? A synchronous client is also available and mirrors the async API:

```python
from nebulagraph_python import NebulaClient

with NebulaClient(
    hosts=["127.0.0.1:9669"],
    username="root",
    password="NebulaGraph01",
) as client:
    result = client.execute("RETURN 1 AS a, 2 AS b")
    result.print()
```

## Manual Initialization and Closing

If you prefer manual lifecycle control, you can explicitly open and close clients.

- Async version:

```python
import asyncio
from nebulagraph_python.client import NebulaAsyncClient

async def main() -> None:
    client = await NebulaAsyncClient.connect(
        hosts=["127.0.0.1:9669"],
        username="root",
        password="NebulaGraph01",
    )
    try:
        result = await client.execute("RETURN 1 AS a, 2 AS b")
        result.print()
    finally:
        await client.close()

asyncio.run(main())
```

## Console Tools

Run `ngcli --help` to get the help message. An example to connect to NebulaGraph is as follows:

```bash
ngcli -h 127.0.0.1:9669 -u root -p NebulaGraph01
```