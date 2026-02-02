#!/usr/bin/env python3
"""
Example demonstrating the usage of AsyncNebulaClient

This example shows how to use the async interface for NebulaGraph operations.
"""

import asyncio
from nebulagraph_python.client import AsyncNebulaClient


async def main():
    """Main async function demonstrating AsyncNebulaClient usage"""

    # Method 1: Direct initialization
    print("=== Method 1: Direct Initialization ===")
    client = AsyncNebulaClient(
        addresses="127.0.0.1:9669",
        user_name="root",
        password="nebula",
        connect_timeout_ms=3000,
        request_timeout_ms=30000,
    )

    try:
        # Initialize the connection (this is async)
        await client._init_client()
        print(f"✓ Connected to: {client.get_host()}")
        print(f"✓ Session ID: {client.get_session_id()}")
        print(f"✓ Server version: {client.get_version()}")

        # Execute a query
        result = await client.execute("SHOW HOSTS")
        print(f"✓ Query executed successfully, rows: {result.row_size()}")

        # Execute with custom timeout
        result = await client.execute_with_timeout("SHOW SPACES", 5000)
        print(f"✓ Spaces query executed, rows: {result.row_size()}")

        # Ping the server
        is_alive = await client.ping()
        print(f"✓ Server is alive: {is_alive}")

    except Exception as e:
        print(f"✗ Error: {e}")
    finally:
        await client.close()
        print("✓ Client closed\n")

    # Method 2: Direct initialization with TLS disabled
    print("=== Method 2: Direct Initialization with TLS ===")
    client = AsyncNebulaClient(
        addresses="127.0.0.1:9669",
        user_name="root",
        password="nebula",
        connect_timeout_ms=3000,
        request_timeout_ms=30000,
        enable_tls=False
    )

    try:
        await client._init_client()
        print(f"✓ Connected to: {client.get_host()}")

        # Execute multiple queries concurrently
        queries = [
            "SHOW HOSTS",
            "SHOW SPACES",
            "SHOW TAGS",
        ]

        results = await asyncio.gather(*[client.execute(q) for q in queries])
        for i, (query, result) in enumerate(zip(queries, results), 1):
            print(f"✓ Query {i} ({query}): {result.row_size()} rows")

    except Exception as e:
        print(f"✗ Error: {e}")
    finally:
        await client.close()
        print("✓ Client closed\n")
        print("✓ Client closed\n")

    # Method 3: Async context manager pattern (you can create your own)
    print("=== Method 3: Async Context Manager Pattern ===")

    async with AsyncNebulaClient(
        addresses="127.0.0.1:9669",
        user_name="root",
        password="nebula"
    ) as client:
        await client._init_client()
        print(f"✓ Connected with context manager: {client.get_host()}")

        # Simple query
        result = await client.execute("SHOW HOSTS")
        print(f"✓ Result: {result.row_size()} rows")


# Add async context manager support to AsyncNebulaClient
async def async_context_manager_example():
    """Example of creating an async context manager"""

    async def get_async_client(addresses, user_name, password):
        """Factory function for creating and initializing an async client"""
        client = AsyncNebulaClient(
            addresses=addresses,
            user_name=user_name,
            password=password
        )
        await client._init_client()
        return client

    # You can also use this pattern:
    async def with_async_client(addresses, user_name, password, callback):
        """Execute a callback with an async client"""
        client = AsyncNebulaClient(
            addresses=addresses,
            user_name=user_name,
            password=password
        )
        try:
            await client._init_client()
            return await callback(client)
        finally:
            await client.close()

    # Usage example
    async def query_callback(client):
        """Callback to execute queries"""
        result = await client.execute("SHOW HOSTS")
        return result.row_size()

    try:
        row_count = await with_async_client(
            "127.0.0.1:9669",
            "root",
            "nebula",
            query_callback
        )
        print(f"✓ Callback pattern: {row_count} rows")
    except Exception as e:
        print(f"✗ Callback pattern error (expected if server not running): {e}")


if __name__ == "__main__":
    print("AsyncNebulaClient Usage Examples")
    print("=" * 60)
    print()

    try:
        # Run the main example
        asyncio.run(main())

        # Run the context manager example
        asyncio.run(async_context_manager_example())

    except Exception as e:
        print(f"\nNote: Make sure NebulaGraph server is running at 127.0.0.1:9669")
        print(f"Error details: {e}")