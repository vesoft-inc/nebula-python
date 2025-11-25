
# NebulaGraph Python Concurrency Guide

This guide explains how to achieve concurrent query execution using the NebulaGraph Python client with `AsyncClient` and `asyncio.gather`.

## Core Concept: How Concurrency Works

### The Session Limitation
- **One session = One query**: Each NebulaGraph session can only execute one query at a time
- **Sequential execution**: Without multiple sessions, queries run one after another
- **The solution**: Use session pools to create multiple sessions for concurrent execution

### Session Pool Architecture
The session pool creates and maintains multiple sessions **to each host**:

- **Per-host pooling**: If you have 3 hosts and pool size of 5, you get 5 sessions **per host** (not 5 total)
- **Multiple sessions = Concurrent queries**: Having multiple sessions allows executing multiple queries simultaneously
- **Total concurrency calculation**: Maximum concurrent queries = `number_of_hosts × session_pool_size`

**Example**: With 2 hosts and pool size of 3:
- Host 1: 3 sessions → can handle 3 concurrent queries
- Host 2: 3 sessions → can handle 3 concurrent queries  
- **Total**: 6 queries can execute concurrently across both hosts

## Complete Example

```python
import asyncio
from nebulagraph_python import NebulaAsyncClient, SessionPoolConfig, ConnectionConfig, SessionConfig

async def concurrent_example():
    # Create client with session pool for concurrency
    async with await NebulaAsyncClient.connect(
        hosts=["127.0.0.1:9669", "127.0.0.1:9670"],  # Multiple hosts for HA
        username="root",
        password="NebulaGraph01",
        session_pool_config=SessionPoolConfig(
            size=3,           # Pool of 3 sessions per host
            wait_timeout=5.0  # Wait up to 5 seconds for available session
        ),
        conn_config=ConnectionConfig(
            connect_timeout=10.0,    # Connection timeout
            request_timeout=30.0,    # Query timeout
            ping_before_execute=True # Ping server before each query to ensure connection health
        ),
        session_config=SessionConfig()  # Default session settings
    ) as client:
        # Total concurrency: 2 hosts × 3 sessions = 6 concurrent queries
        
        # Execute multiple queries concurrently
        tasks = [
            client.execute_py("RETURN {{i}}", {"i": i}) 
            for i in range(6)
        ]
        
        results = await asyncio.gather(*tasks)
        
        for result in results:
            result.print()

# Run the example
asyncio.run(concurrent_example())
```

## Understanding Timeout Values

The client uses three different timeouts that apply at different stages:

### 1. Session Pool Timeout (`wait_timeout = 5.0s`)
- **When**: Requesting a session from the pool
- **What**: How long to wait if all sessions are busy
- **Example**: You have 3 sessions, all executing queries. The 4th query waits up to 5 seconds for a session to become available
- **If exceeded**: Raises exception before query starts

### 2. Connection Timeout (`connect_timeout = 10.0s`)
- **When**: Establishing connection to NebulaGraph server
- **What**: How long to wait for TCP connection and authentication
- **Example**: When client first connects to `127.0.0.1:9669` or reconnects after failure
- **If exceeded**: Tries next host or raises connection error

### 3. Request Timeout (`request_timeout = 30.0s`)
- **When**: Executing a query on established session
- **What**: How long to wait for query completion
- **Example**: During actual `MATCH`, `INSERT`, or other GQL execution
- **If exceeded**: Cancels query and raises timeout error

## Connection Health Check (`ping_before_execute`)

The `ping_before_execute` parameter in `ConnectionConfig` controls whether the client pings the server before executing each query:

- **Purpose**: Ensures the connection is still alive before sending queries
- **When enabled**: Client sends a ping to the server before each query execution
- **Benefits**: Detects connection failures early and can trigger automatic reconnection
- **Trade-off**: Adds slight overhead (extra network round-trip) but improves reliability
- **Recommended**: Enable for production environments where connection stability is important

### Query Execution Flow
```
Client Initialization:
- Establish connections to all hosts → connect_timeout (10s)

Per Query Execution:
1. Round-robin + ping to find available connection → ping overhead
2. Get session from that connection's pool → wait_timeout (5s)
3. Execute query on the session → request_timeout (30s)
```

## Why This Works

1. **True Concurrency**: `asyncio.gather` executes all queries simultaneously
2. **Scalable**: Adding hosts multiplies total concurrency capacity
3. **Efficient**: Connections are reused

## Best Practices

- **Pool Size**: Match `SessionPoolConfig.size` to your expected concurrency level. It can be a vary large value to allow unlimited concurrency.
- **Multiple Hosts**: Use multiple NebulaGraph hosts for high availability and increased concurrency
- **Ping Before Execute**: Enable `ping_before_execute` to ensure connection health before executing each query
- **Close the client**: Always call `client.close()` when you are done to release sessions and connections, preventing server-side connection exhaustion.

## Synchronous Client Concurrency

If you prefer a blocking API, you can achieve concurrency with the synchronous client by combining a session pool and threads. The same "one session = one query" rule applies; concurrency comes from multiple sessions in the pool.

```python
from concurrent.futures import ThreadPoolExecutor
from nebulagraph_python import NebulaClient, SessionPoolConfig

with NebulaClient(
    hosts=["127.0.0.1:9669"],
    username="root",
    password="NebulaGraph01",
    session_pool_config=SessionPoolConfig(),  # enables multiple sessions per host
) as client, ThreadPoolExecutor(max_workers=8) as executor:
    futures = [
        executor.submit(client.execute_py, "RETURN {{idx}} AS idx", {"idx": x})
        for x in range(8)
    ]
    for f in futures:
        print(f.result().as_primitive_by_column())
```