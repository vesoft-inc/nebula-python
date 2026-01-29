from nebulagraph_python import NebulaClient, NebulaPool, NebulaPoolConfig


def sync_client_example():
    """Example using synchronous NebulaClient"""

    # Create client using direct initialization
    # Note: NebulaClient automatically establishes connection in __init__
    # and inherits from NebulaBaseExecutor, so it can use execute_py() directly
    client = NebulaClient(
        addresses="127.0.0.1:9669",
        user_name="root",
        password="nebula",
        connect_timeout_ms=3000,
        request_timeout_ms=30000
    )

    try:
        print(f"Connected to NebulaGraph at {client.get_host()}")
        print(f"Session ID: {client.get_session_id()}")
        print(f"Server version: {client.get_version()}")

        # Execute simple query
        result = client.execute("SHOW HOSTS")
        print("SHOW HOSTS result:")
        result.print()

        # Execute query with custom timeout
        result = client.execute_with_timeout("SHOW SPACES", 5000)
        print("SHOW SPACES result:")
        result.print()

        # Test ping
        is_alive = client.ping()
        print(f"Server is alive: {is_alive}")

        # execute_py example (NebulaClient now supports this directly)
        query = """
        RETURN {{v1}} as v1, {{v2}} as v2, {{v3}} as v3
        """
        args = {"v1": 1, "v2": "alice", "v3": [True, False, True]}

        res = client.execute_py(query, args)
        print("execute_py result:")
        res.print()

        # Get the first row in primitive type
        row = res.one().as_primitive()
        assert row == args
        print(f"Row as primitive: {row}")

        # Get result in column-oriented primitive type
        print(f"Column-oriented: {res.as_primitive_by_column()}")

        # Get result in row-oriented primitive type
        print(f"Row-oriented: {list(res.as_primitive_by_row())}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()
        print("Client closed\n")


def async_client_example():
    """Example using AsyncNebulaClient"""
    import asyncio
    from nebulagraph_python.client import AsyncNebulaClient

    async def run():
        # Create async client using direct initialization
        # Note: AsyncNebulaClient does NOT automatically connect in __init__
        # You must call _init_client() manually before using the client
        client = AsyncNebulaClient(
            addresses="127.0.0.1:9669",
            user_name="root",
            password="nebula",
            connect_timeout_ms=3000,
            request_timeout_ms=30000
        )

        try:
            # Initialize connection (required for AsyncNebulaClient)
            await client._init_client()
            print(f"Connected to NebulaGraph at {client.get_host()}")
            print(f"Session ID: {client.get_session_id()}")
            print(f"Server version: {client.get_version()}")

            # Execute simple query
            result = await client.execute("SHOW HOSTS")
            print("SHOW HOSTS result:")
            result.print()

            # Execute query with custom timeout
            result = await client.execute_with_timeout("SHOW SPACES", 5000)
            print("SHOW SPACES result:")
            result.print()

            # Test ping
            is_alive = await client.ping()
            print(f"Server is alive: {is_alive}")

            # Execute multiple queries concurrently
            queries = [
                "SHOW HOSTS",
                "SHOW SPACES",
                "SHOW TAGS",
            ]

            results = await asyncio.gather(*[client.execute(q) for q in queries])
            for i, (query, result) in enumerate(zip(queries, results), 1):
                print(f"Query {i} ({query}): {result.row_size()} rows")

        except Exception as e:
            print(f"Error: {e}")
        finally:
            await client.close()
            print("Async client closed\n")

    asyncio.run(run())


def pool_example():
    """Example using NebulaPool for connection pooling"""

    # Create pool configuration
    config = NebulaPoolConfig(
        addresses="127.0.0.1:9669",
        username="root",
        password="nebula",
        max_client_size=10,
        min_client_size=2,
        max_wait_ms=5000,
        graph="movie",  # Optional: set default graph
    )

    pool = NebulaPool(config)

    try:
        print("Pool created successfully")
        print(f"Active sessions: {pool.get_active_sessions()}")
        print(f"Idle sessions: {pool.get_idle_sessions()}")

        # Get a client from the pool
        client = pool.get_client()

        print(f"Got client, session ID: {client.get_session_id()}")
        print(f"Active sessions: {pool.get_active_sessions()}")

        # Execute queries
        result = client.execute("SHOW HOSTS")
        print("Query result:")
        result.print()

        # Use execute_py (NebulaClient now supports this directly)
        res = client.execute_py("RETURN 1 AS num")
        print("execute_py result:")
        res.print()

        # Return the client to the pool
        pool.return_client(client)
        print(f"Returned client, idle sessions: {pool.get_idle_sessions()}")

        # Get another client
        client2 = pool.get_client()
        print(f"Got another client, session ID: {client2.get_session_id()}")

        result = client2.execute("SHOW SPACES")
        result.print()

        pool.return_client(client2)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        pool.close()
        print("Pool closed\n")


def multi_threaded_pool_example():
    """Example using NebulaPool with multiple threads"""
    from concurrent.futures import ThreadPoolExecutor
    from nebulagraph_python import NebulaPool, NebulaPoolConfig

    def query_task(pool, idx):
        """Task to execute a query"""
        client = pool.get_client()
        try:
            result = client.execute(f"RETURN {idx} AS num")
            return result.as_primitive_by_column()
        finally:
            pool.return_client(client)

    # Create pool
    config = NebulaPoolConfig(
        addresses="127.0.0.1:9669",
        username="root",
        password="nebula",
        max_client_size=10,
        min_client_size=2,
    )

    with NebulaPool(config) as pool:
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(query_task, pool, i) for i in range(10)]
            for future in futures:
                print(future.result())


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    logging.getLogger("nebulagraph_python").setLevel(logging.INFO)

    print("=" * 60)
    print("Synchronous Client Example")
    print("=" * 60)
    sync_client_example()

    print("=" * 60)
    print("Asynchronous Client Example")
    print("=" * 60)
    async_client_example()

    print("=" * 60)
    print("Connection Pool Example")
    print("=" * 60)
    pool_example()

    print("=" * 60)
    print("Multi-threaded Pool Example")
    print("=" * 60)
    multi_threaded_pool_example()
