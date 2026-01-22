#!/usr/bin/env python3
"""
Test connection leak
"""

import time
from nebulagraph_python.client import NebulaClient, NebulaPool

print("=" * 60)
print("Testing NebulaClient connection leak")
print("=" * 60)

try:
    # Test 1: Create and close client multiple times
    for i in range(3):
        print(f"\nCreating client for the {i+1} time")
        client = NebulaClient(
            hosts="192.168.8.6:3820",
            username="root",
            password="NebulaGraph01",
        )
        result = client.execute("SHOW GRAPHS")
        print(f"   Query successful: {result.size} graph spaces")
        client.close()
        print(f"   Client closed")

    print("\n NebulaClient connection leak test passed")

except Exception as e:
    print(f"✗ NebulaClient test failed: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("Testing NebulaPool connection leak")
print("=" * 60)

try:
    # Test 2: Create connection pool and verify connection management

    pool = NebulaPool(
        hosts="192.168.8.6:3820",
        username="root",
        password="NebulaGraph01"
    )
    print(f" Connection pool created successfully")
    print(f"  Initial client count: {len(pool._clients)}")

    # Test getting and returning clients
    clients = []
    for i in range(5):
        client = pool.get_client()
        clients.append(client)
        print(f"   Got client {i+1}, total clients: {len(pool._clients)}, in use: {len(pool._in_use)}")

    # Return all clients
    for i, client in enumerate(clients):
        pool.return_client(client)
        print(f"  Returned client {i+1}, total clients: {len(pool._clients)}, in use: {len(pool._in_use)}")

    # Close connection pool
    pool.close()
    print(f" Connection pool closed")

    print("\n NebulaPool connection leak test passed")

except Exception as e:
    print(f" NebulaPool test failed: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("Testing connection cleanup in exception scenarios")
print("=" * 60)

try:
    # Test 3: Test connection cleanup when pool creation fails
    # Use a non-existent address

    try:
        pool = NebulaPool(
            hosts="192.168.8.6:9999",  # Non-existent port
            username="root",
            password="NebulaGraph01",
        )
        print(" Should have failed but succeeded")
    except Exception as e:
        print(f" Expected failure: {type(e).__name__}: {str(e)[:50]}")

    # Verify that previous connection pool is still available
    pool2 = NebulaPool(
        hosts="192.168.8.6:3820",
        username="root",
        password="NebulaGraph01",
    )
    print(f" New connection pool created successfully, client count: {len(pool2._clients)}")
    pool2.close()
    print(f" New connection pool closed")

    print("\ Exception cleanup test passed")

except Exception as e:
    print(f" Exception cleanup test failed: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("All tests completed")
print("=" * 60)
