#!/usr/bin/env python3
"""Test script for AsyncNebulaClient functionality"""

import asyncio
from nebulagraph_python.client import AsyncNebulaClient


async def test_async_client():
    """Test AsyncNebulaClient basic operations"""
    # Create async client using direct initialization
    client = AsyncNebulaClient(
        addresses="127.0.0.1:9669",
        user_name="root",
        password="nebula",
        connect_timeout_ms=3000,
        request_timeout_ms=30000,
    )

    try:
        # Initialize connection
        await client._init_client()
        print(f"Connected to NebulaGraph at {client.get_host()}")
        print(f"Session ID: {client.get_session_id()}")
        print(f"Server version: {client.get_version()}")

        # Test ping
        ping_result = await client.ping()
        print(f"Ping result: {ping_result}")

        # Test execute
        result = await client.execute("SHOW HOSTS")
        print("Query result:")
        print(result)

        # Test execute_with_timeout
        result2 = await client.execute_with_timeout("SHOW SPACES", 5000)
        print("Spaces result:")
        print(result2)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close client
        await client.close()
        print("Client closed")


async def test_async_with_tls():
    """Test AsyncNebulaClient with TLS disabled"""
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
        print(f"TLS test - Connected to {client.get_host()}")
        print(f"Session ID: {client.get_session_id()}")
    except Exception as e:
        print(f"TLS test error: {e}")
    finally:
        await client.close()


if __name__ == "__main__":
    print("Testing AsyncNebulaClient...")
    print("=" * 50)
    asyncio.run(test_async_client())
    print("\n" + "=" * 50)
    print("Testing AsyncNebulaClient with TLS...")
    asyncio.run(test_async_with_tls())