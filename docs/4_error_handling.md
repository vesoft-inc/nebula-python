## Error handling

This library separates errors into two categories:

- **Remote errors**: returned by NebulaGraph when a statement executes but fails on the server side.
- **Client-side errors**: thrown locally by the SDK for connectivity, authentication, pooling, execution flow, or unexpected situations.

The relevant definitions live in the `nebulagraph_python.error` module.

### Remote errors: `NebulaGraphRemoteError`

- Class: `NebulaGraphRemoteError(code, message, result=None)`
  - **code**: `ErrorCode` (from `nebulagraph_python._error_code`)
  - **message**: textual description from the server
  - **result**: optional `ResultSet` that triggered the error
- Emission point: both sync and async clients call `.raise_on_error()` on the returned `ResultSet` inside `execute(...)`. If the server indicates failure, `NebulaGraphRemoteError` is raised.

```python
import asyncio
from nebulagraph_python.client.client import NebulaAsyncClient
from nebulagraph_python.error import NebulaGraphRemoteError, ErrorCode

async def main():
    async with await NebulaAsyncClient.connect(
        hosts="localhost:9669", username="root", password="NebulaGraph01"
    ) as client:
        try:
            rs = await client.execute_py("USE not_exist_graph RETURN 1")
        except NebulaGraphRemoteError as e:
            # Check concrete error codes against enum, e.g. CATALOG_GRAPH_NOT_FOUND
            if e.code is ErrorCode.CATALOG_GRAPH_NOT_FOUND:
                print("Catched! graph not found:", e.message)
            else:
                print("Catched! remote error:", e.code, e.message)

asyncio.run(main())
```

### Client-side errors

All client-side errors inherit from `NebulaGraphClientError`.

- `InternalError` — unrecoverable internal states or unexpected inputs.
- `ConnectingError` — failures during establishing connections to the server.
- `AuthenticatingError` — authentication failures.
- `ExecutingError` — failures while sending or awaiting a statement (non-server error path).
- `PoolError` — pool is unhealthy or cannot provide a connection/session.

Surface them as-is to let callers distinguish root causes.
