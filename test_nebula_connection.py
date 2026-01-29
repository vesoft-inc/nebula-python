#!/usr/bin/env python3
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

"""Simple script to test NebulaGraph connection"""

import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from nebulagraph_python import NebulaPool, NebulaPoolConfig

# Configuration
NEBULA_HOSTS = os.getenv("NEBULA_HOSTS", "192.168.8.6:3820")
NEBULA_USER = os.getenv("NEBULA_USER", "root")
NEBULA_PASSWORD = os.getenv("NEBULA_PASSWORD", "NebulaGraph01")

def test_connection():
    """Test basic connection to NebulaGraph"""
    print(f"Testing connection to NebulaGraph at {NEBULA_HOSTS}...")
    print(f"User: {NEBULA_USER}")

    try:
        config = NebulaPoolConfig(
            addresses=NEBULA_HOSTS,
            username=NEBULA_USER,
            password=NEBULA_PASSWORD,
            max_client_size=1,
            min_client_size=1,
        )
        pool = NebulaPool(config)
        client = pool.get_client()

        # Test simple query
        result = client.execute("RETURN 1 AS num")
        if result.is_succeeded:
            print("✓ Connection successful!")
            print(f"✓ Query executed successfully: {result}")
        else:
            print(f"✗ Query failed: {result.error_msg()}")

        pool.return_client(client)
        pool.close()
        return True

    except Exception as e:
        print(f"✗ Connection failed: {e}")
        print("\nPlease ensure:")
        print("1. NebulaGraph server is running")
        print("2. Server address is correct (default: 127.0.0.1:9669)")
        print("3. Username and password are correct")
        print("\nYou can set custom credentials using environment variables:")
        print("  export NEBULA_HOSTS='127.0.0.1:9669'")
        print("  export NEBULA_USER='root'")
        print("  export NEBULA_PASSWORD='nebula'")
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
