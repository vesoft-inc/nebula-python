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

import asyncio
import os
import pytest

from nebulagraph_python import (
    NebulaClient,
    NebulaPool,
    NebulaPoolConfig,
)
from nebulagraph_python.client import AsyncNebulaClient

# Get test configuration from environment variables, or use default values
NEBULA_HOST = os.getenv("NEBULA_HOST", "192.168.8.6")
NEBULA_PORT = os.getenv("NEBULA_PORT", "3820")
NEBULA_USER = os.getenv("NEBULA_USER", "root")
NEBULA_PASSWORD = os.getenv("NEBULA_PASSWORD", "NebulaGraph01")

NEBULA_ADDRESS = f"{NEBULA_HOST}:{NEBULA_PORT}"


class TestConnectionIntegration:
    """Connection integration tests - Test Connection functionality"""

    def test_connection_basic(self):
        """Test basic connection"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )
        assert client is not None
        client.close()

    def test_connection_ping(self):
        """Test connection ping functionality"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )
        assert client.ping() is True
        client.close()

    def test_connection_execute_simple_query(self):
        """Test executing simple query"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )
        result = client.execute("RETURN 1")
        assert result is not None
        assert result.is_succeeded
        client.close()

    def test_connection_execute_show_hosts(self):
        """Test executing SHOW HOSTS command"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )
        # NebulaGraph 5.0 uses different syntax
        result = client.execute("SHOW HOSTS GRAPH")
        assert result is not None
        assert result.is_succeeded
        client.close()

    def test_connection_context_manager(self):
        """Test context manager"""
        with NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        ) as client:
            assert client is not None
            result = client.execute("RETURN 1")
            assert result.is_succeeded


class TestAsyncConnectionIntegration:
    """Async connection integration tests - Test AsyncConnection functionality"""

    @pytest.mark.asyncio
    async def test_async_connection_basic(self):
        """Test basic async connection"""
        client = AsyncNebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )
        await client._init_client()
        assert client is not None
        await client.close()

    @pytest.mark.asyncio
    async def test_async_connection_execute(self):
        """Test async query execution"""
        client = AsyncNebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )
        await client._init_client()
        result = await client.execute("RETURN 1")
        assert result is not None
        assert result.is_succeeded
        await client.close()

    @pytest.mark.asyncio
    async def test_async_connection_show_hosts(self):
        """Test async SHOW HOSTS execution"""
        client = AsyncNebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )
        await client._init_client()
        result = await client.execute("SHOW HOSTS")
        assert result is not None
        assert result.is_succeeded
        await client.close()


class TestNebulaPoolIntegration:
    """Connection pool integration tests"""

    def test_nebula_pool_basic(self):
        """Test basic connection pool"""
        pool_config = NebulaPoolConfig(
            addresses=NEBULA_ADDRESS,
            user_name=NEBULA_USER,
            password=NEBULA_PASSWORD,
            max_client_size=3,
            min_client_size=1,
            max_wait_ms=10000,
        )
        pool = NebulaPool(pool_config)
        assert pool is not None

        client = pool.get_client()
        result = client.execute("RETURN 1")
        assert result.is_succeeded
        pool.return_client(client)

        pool.close()

    def test_nebula_pool_get_client_and_return(self):
        """Test getting and returning client"""
        pool_config = NebulaPoolConfig(
            addresses=NEBULA_ADDRESS,
            user_name=NEBULA_USER,
            password=NEBULA_PASSWORD,
            max_client_size=2,
            min_client_size=1,
            max_wait_ms=10000,
        )
        pool = NebulaPool(pool_config)

        client = pool.get_client()
        assert client is not None

        result = client.execute("RETURN 1")
        assert result.is_succeeded

        pool.return_client(client)
        pool.close()

    def test_nebula_pool_concurrent(self):
        """Test connection pool concurrent access"""
        import threading
        import time

        pool_config = NebulaPoolConfig(
            addresses=NEBULA_ADDRESS,
            user_name=NEBULA_USER,
            password=NEBULA_PASSWORD,
            max_client_size=5,
            min_client_size=2,
            max_wait_ms=10000,
        )
        pool = NebulaPool(pool_config)

        results = []
        errors = []

        def execute_query(thread_id):
            try:
                client = pool.get_client()
                result = client.execute("RETURN 1")
                results.append(thread_id)
                pool.return_client(client)
                time.sleep(0.1)
            except Exception as e:
                errors.append((thread_id, e))

        threads = []
        for i in range(5):
            thread = threading.Thread(target=execute_query, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(errors) == 0
        assert len(results) == 5
        pool.close()

    def test_nebula_pool_context_manager(self):
        """Test connection pool context manager"""
        pool_config = NebulaPoolConfig(
            addresses=NEBULA_ADDRESS,
            user_name=NEBULA_USER,
            password=NEBULA_PASSWORD,
            max_client_size=2,
            min_client_size=1,
            max_wait_ms=10000,
        )
        with NebulaPool(pool_config) as pool:
            client = pool.get_client()
            result = client.execute("RETURN 1")
            assert result.is_succeeded
            pool.return_client(client)


class TestGraphOperations:
    """Graph operations tests"""

    def test_create_space(self):
        """Test creating graph space"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # Drop existing graph space if exists
        client.execute("DROP SPACE IF EXISTS test_space")

        # Create graph space
        result = client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        assert result.is_succeeded

        # Use graph space
        result = client.execute("USE test_space")
        assert result.is_succeeded

        client.close()

    def test_create_tag(self):
        """Test creating tag"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # Ensure graph space exists
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")

        # Create tag
        result = client.execute("CREATE TAG IF NOT EXISTS person(name string, age int)")
        assert result.is_succeeded

        client.close()

    def test_create_edge(self):
        """Test creating edge type"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # Ensure graph space exists
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")

        # Create edge type
        result = client.execute("CREATE EDGE IF NOT EXISTS follow(degree int)")
        assert result.is_succeeded

        client.close()

    def test_insert_vertex(self):
        """Test inserting vertex"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # Prepare graph space
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")
        client.execute("CREATE TAG IF NOT EXISTS person(name string, age int)")

        # Insert vertex
        result = client.execute('INSERT VERTEX person(name, age) VALUES "1":("Tom", 18), "2":("Jerry", 20)')
        assert result.is_succeeded

        client.close()

    def test_insert_edge(self):
        """Test inserting edge"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # Prepare graph space
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")
        client.execute("CREATE TAG IF NOT EXISTS person(name string, age int)")
        client.execute("CREATE EDGE IF NOT EXISTS follow(degree int)")
        client.execute('INSERT VERTEX person(name, age) VALUES "1":("Tom", 18), "2":("Jerry", 20)')

        # Insert edge
        result = client.execute('INSERT EDGE follow(degree) VALUES "1"->"2":(90)')
        assert result.is_succeeded

        client.close()

    def test_query_vertex(self):
        """Test querying vertex"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # Prepare data
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")
        client.execute("CREATE TAG IF NOT EXISTS person(name string, age int)")
        client.execute('INSERT VERTEX person(name, age) VALUES "1":("Tom", 18)')

        # Query vertex
        result = client.execute('FETCH PROP ON person "1" YIELD vertex as v')
        assert result.is_succeeded

        client.close()

    def test_query_edge(self):
        """Test querying edge"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # Prepare data
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")
        client.execute("CREATE TAG IF NOT EXISTS person(name string, age int)")
        client.execute("CREATE EDGE IF NOT EXISTS follow(degree int)")
        client.execute('INSERT VERTEX person(name, age) VALUES "1":("Tom", 18), "2":("Jerry", 20)')
        client.execute('INSERT EDGE follow(degree) VALUES "1"->"2":(90)')

        # Query edge
        result = client.execute('FETCH PROP ON follow "1"->"2" YIELD edge as e')
        assert result.is_succeeded

        client.close()

    def test_complex_query(self):
        """Test complex query"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # Prepare data
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")
        client.execute("CREATE TAG IF NOT EXISTS person(name string, age int)")
        client.execute("CREATE EDGE IF NOT EXISTS follow(degree int)")
        client.execute('INSERT VERTEX person(name, age) VALUES "1":("Tom", 18), "2":("Jerry", 20), "3":("Alice", 22)')
        client.execute('INSERT EDGE follow(degree) VALUES "1"->"2":(90), "2"->"3":(80)')

        # Complex query: find people Tom follows
        result = client.execute('GO FROM "1" OVER follow YIELD $$.person.name AS name, $$.person.age AS age')
        assert result.is_succeeded

        client.close()


class TestErrorHandling:
    """Error handling tests"""

    def test_invalid_query(self):
        """Test invalid query"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        result = client.execute("INVALID QUERY")
        assert not result.is_succeeded

        client.close()

    def test_wrong_credentials(self):
        """Test wrong credentials"""
        with pytest.raises(Exception):
            client = NebulaClient(
                NEBULA_ADDRESS,
                "wrong_user",
                "wrong_password",
            )
            client.close()

    def test_connection_timeout(self):
        """Test connection timeout"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            connect_timeout_ms=1000,
        )
        # Should connect successfully
        assert client.ping()
        client.close()


class TestPerformance:
    """Performance tests"""

    def test_batch_insert(self):
        """Test batch insert"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # Prepare graph space
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")
        client.execute("CREATE TAG IF NOT EXISTS person(name string, age int)")

        # Batch insert
        vertices = []
        for i in range(100):
            vertices.append(f'"{i}":("Person{i}", {20 + i % 30})')

        query = f'INSERT VERTEX person(name, age) VALUES {", ".join(vertices)}'
        result = client.execute(query)
        assert result.is_succeeded

        client.close()

    def test_concurrent_queries(self):
        """Test concurrent queries"""
        import threading

        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        results = []
        errors = []

        def execute_query(thread_id):
            try:
                result = client.execute("RETURN 1")
                results.append(thread_id)
            except Exception as e:
                errors.append((thread_id, e))

        threads = []
        for i in range(10):
            thread = threading.Thread(target=execute_query, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        assert len(errors) == 0
        assert len(results) == 10
        client.close()
