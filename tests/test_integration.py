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
    NebulaAsyncClient,
    NebulaPool,
    NebulaPoolConfig,
    SessionConfig,
    SessionPoolConfig,
)

# 从环境变量获取测试配置，如果没有则使用默认值
NEBULA_HOST = os.getenv("NEBULA_HOST", "192.168.8.6")
NEBULA_PORT = os.getenv("NEBULA_PORT", "3820")
NEBULA_USER = os.getenv("NEBULA_USER", "root")
NEBULA_PASSWORD = os.getenv("NEBULA_PASSWORD", "NebulaGraph01")

NEBULA_ADDRESS = f"{NEBULA_HOST}:{NEBULA_PORT}"


class TestConnectionIntegration:
    """实际连接测试 - 测试Connection功能"""

    def test_connection_basic(self):
        """测试基本连接"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )
        assert client is not None
        client.close()

    def test_connection_ping(self):
        """测试连接ping功能"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )
        assert client.ping() is True
        client.close()

    def test_connection_execute_simple_query(self):
        """测试执行简单查询"""
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
        """测试执行SHOW HOSTS命令"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )
        # NebulaGraph 5.0 使用不同的语法
        result = client.execute("SHOW HOSTS GRAPH")
        assert result is not None
        assert result.is_succeeded
        client.close()

    def test_connection_context_manager(self):
        """测试上下文管理器"""
        with NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        ) as client:
            assert client is not None
            result = client.execute("RETURN 1")
            assert result.is_succeeded

    def test_connection_with_session_config(self):
        """测试带会话配置的连接"""
        session_config = SessionConfig()
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            session_config=session_config,
        )
        assert client is not None
        result = client.execute("RETURN 1")
        assert result.is_succeeded
        client.close()


class TestAsyncConnectionIntegration:
    """异步连接测试 - 测试AsyncConnection功能"""

    @pytest.mark.asyncio
    async def test_async_connection_basic(self):
        """测试基本异步连接"""
        client = await NebulaAsyncClient.connect(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )
        assert client is not None
        await client.close()

    @pytest.mark.asyncio
    async def test_async_connection_execute(self):
        """测试异步执行查询"""
        client = await NebulaAsyncClient.connect(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )
        result = await client.execute("RETURN 1")
        assert result is not None
        assert result.is_succeeded
        await client.close()

    @pytest.mark.asyncio
    async def test_async_connection_show_hosts(self):
        """测试异步执行SHOW HOSTS"""
        client = await NebulaAsyncClient.connect(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )
        result = await client.execute("SHOW HOSTS")
        assert result is not None
        assert result.is_succeeded
        await client.close()

    @pytest.mark.asyncio
    async def test_async_connection_context_manager(self):
        """测试异步上下文管理器"""
        async with await NebulaAsyncClient.connect(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        ) as client:
            assert client is not None
            result = await client.execute("RETURN 1")
            assert result.is_succeeded


class TestSessionPoolIntegration:
    """会话池集成测试"""

    def test_session_pool_basic(self):
        """测试基本会话池"""
        pool_config = SessionPoolConfig(size=3)
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            session_pool_config=pool_config,
        )
        assert client is not None

        # 执行多个查询
        for i in range(5):
            result = client.execute("RETURN 1")
            assert result.is_succeeded

        client.close()

    def test_session_pool_concurrent(self):
        """测试会话池并发访问"""
        import threading
        import time

        pool_config = SessionPoolConfig(size=3)
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            session_pool_config=pool_config,
        )

        results = []
        errors = []

        def execute_query(thread_id):
            try:
                result = client.execute("RETURN 1")
                results.append(thread_id)
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
        client.close()

    def test_session_pool_borrow_session(self):
        """测试借用会话"""
        pool_config = SessionPoolConfig(size=2)
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            session_pool_config=pool_config,
        )

        with client.borrow() as session:
            result = session.execute("RETURN 1")
            assert result.is_succeeded

        client.close()


class TestAsyncSessionPoolIntegration:
    """异步会话池集成测试"""

    @pytest.mark.asyncio
    async def test_async_session_pool_basic(self):
        """测试基本异步会话池"""
        pool_config = SessionPoolConfig(size=3)
        client = await NebulaAsyncClient.connect(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            session_pool_config=pool_config,
        )
        assert client is not None

        # 执行多个查询
        for i in range(5):
            result = await client.execute("RETURN 1")
            assert result.is_succeeded

        await client.close()

    @pytest.mark.asyncio
    async def test_async_session_pool_concurrent(self):
        """测试异步会话池并发访问"""
        pool_config = SessionPoolConfig(size=3)
        client = await NebulaAsyncClient.connect(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            session_pool_config=pool_config,
        )

        async def execute_query(task_id):
            result = await client.execute("RETURN 1")
            assert result.is_succeeded

        tasks = [execute_query(i) for i in range(5)]
        await asyncio.gather(*tasks)

        await client.close()

    @pytest.mark.asyncio
    async def test_async_session_pool_borrow_session(self):
        """测试异步借用会话"""
        pool_config = SessionPoolConfig(size=2)
        client = await NebulaAsyncClient.connect(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            session_pool_config=pool_config,
        )

        async with client.borrow() as session:
            result = await session.execute("RETURN 1")
            assert result.is_succeeded

        await client.close()


class TestNebulaPoolIntegration:
    """连接池集成测试"""

    def test_nebula_pool_basic(self):
        """测试基本连接池"""
        pool_config = NebulaPoolConfig(
            max_client_size=3, min_client_size=1, max_wait=10.0
        )
        pool = NebulaPool(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            pool_config=pool_config,
        )
        assert pool is not None

        result = pool.execute("RETURN 1")
        assert result.is_succeeded

        pool.close()

    def test_nebula_pool_borrow_client(self):
        """测试借用客户端"""
        pool_config = NebulaPoolConfig(
            max_client_size=2, min_client_size=1, max_wait=10.0
        )
        pool = NebulaPool(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            pool_config=pool_config,
        )

        with pool.borrow() as client:
            result = client.execute("RETURN 1")
            assert result.is_succeeded

        pool.close()

    def test_nebula_pool_concurrent(self):
        """测试连接池并发访问"""
        import threading
        import time

        pool_config = NebulaPoolConfig(
            max_client_size=5, min_client_size=2, max_wait=10.0
        )
        pool = NebulaPool(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            pool_config=pool_config,
        )

        results = []
        errors = []

        def execute_query(thread_id):
            try:
                result = pool.execute("RETURN 1")
                results.append(thread_id)
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

    def test_nebula_pool_round_robin(self):
        """测试轮询负载均衡"""
        pool_config = NebulaPoolConfig(
            max_client_size=2, min_client_size=1, max_wait=10.0
        )
        pool = NebulaPool(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            pool_config=pool_config,
        )

        # 执行多个查询，应该轮询使用不同的客户端
        for i in range(4):
            result = pool.execute("RETURN 1")
            assert result.is_succeeded

        pool.close()

    def test_nebula_pool_context_manager(self):
        """测试连接池上下文管理器"""
        pool_config = NebulaPoolConfig(
            max_client_size=2, min_client_size=1, max_wait=10.0
        )
        pool = NebulaPool(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            pool_config=pool_config,
        )
        # NebulaPool不支持上下文管理器，手动关闭
        result = pool.execute("RETURN 1")
        assert result.is_succeeded
        pool.close()

    def test_nebula_pool_get_client_and_return(self):
        """测试获取和返回客户端"""
        pool_config = NebulaPoolConfig(
            max_client_size=2, min_client_size=1, max_wait=10.0
        )
        pool = NebulaPool(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            pool_config=pool_config,
        )

        client = pool.get_client()
        assert client is not None

        result = client.execute("RETURN 1")
        assert result.is_succeeded

        pool.return_client(client)
        pool.close()


class TestGraphOperations:
    """图操作测试"""

    def test_create_space(self):
        """测试创建图空间"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # 删除可能存在的图空间
        client.execute("DROP SPACE IF EXISTS test_space")

        # 创建图空间
        result = client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        assert result.is_succeeded

        # 使用图空间
        result = client.execute("USE test_space")
        assert result.is_succeeded

        client.close()

    def test_create_tag(self):
        """测试创建标签"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # 确保图空间存在
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")

        # 创建标签
        result = client.execute("CREATE TAG IF NOT EXISTS person(name string, age int)")
        assert result.is_succeeded

        client.close()

    def test_create_edge(self):
        """测试创建边类型"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # 确保图空间存在
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")

        # 创建边类型
        result = client.execute("CREATE EDGE IF NOT EXISTS follow(degree int)")
        assert result.is_succeeded

        client.close()

    def test_insert_vertex(self):
        """测试插入顶点"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # 准备图空间
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")
        client.execute("CREATE TAG IF NOT EXISTS person(name string, age int)")

        # 插入顶点
        result = client.execute('INSERT VERTEX person(name, age) VALUES "1":("Tom", 18), "2":("Jerry", 20)')
        assert result.is_succeeded

        client.close()

    def test_insert_edge(self):
        """测试插入边"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # 准备图空间
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")
        client.execute("CREATE TAG IF NOT EXISTS person(name string, age int)")
        client.execute("CREATE EDGE IF NOT EXISTS follow(degree int)")
        client.execute('INSERT VERTEX person(name, age) VALUES "1":("Tom", 18), "2":("Jerry", 20)')

        # 插入边
        result = client.execute('INSERT EDGE follow(degree) VALUES "1"->"2":(90)')
        assert result.is_succeeded

        client.close()

    def test_query_vertex(self):
        """测试查询顶点"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # 准备数据
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")
        client.execute("CREATE TAG IF NOT EXISTS person(name string, age int)")
        client.execute('INSERT VERTEX person(name, age) VALUES "1":("Tom", 18)')

        # 查询顶点
        result = client.execute('FETCH PROP ON person "1" YIELD vertex as v')
        assert result.is_succeeded

        client.close()

    def test_query_edge(self):
        """测试查询边"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # 准备数据
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")
        client.execute("CREATE TAG IF NOT EXISTS person(name string, age int)")
        client.execute("CREATE EDGE IF NOT EXISTS follow(degree int)")
        client.execute('INSERT VERTEX person(name, age) VALUES "1":("Tom", 18), "2":("Jerry", 20)')
        client.execute('INSERT EDGE follow(degree) VALUES "1"->"2":(90)')

        # 查询边
        result = client.execute('FETCH PROP ON follow "1"->"2" YIELD edge as e')
        assert result.is_succeeded

        client.close()

    def test_complex_query(self):
        """测试复杂查询"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # 准备数据
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")
        client.execute("CREATE TAG IF NOT EXISTS person(name string, age int)")
        client.execute("CREATE EDGE IF NOT EXISTS follow(degree int)")
        client.execute('INSERT VERTEX person(name, age) VALUES "1":("Tom", 18), "2":("Jerry", 20), "3":("Alice", 22)')
        client.execute('INSERT EDGE follow(degree) VALUES "1"->"2":(90), "2"->"3":(80)')

        # 复杂查询：查找Tom关注的人
        result = client.execute('GO FROM "1" OVER follow YIELD $$.person.name AS name, $$.person.age AS age')
        assert result.is_succeeded

        client.close()


class TestErrorHandling:
    """错误处理测试"""

    def test_invalid_query(self):
        """测试无效查询"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        result = client.execute("INVALID QUERY")
        assert not result.is_succeeded

        client.close()

    def test_wrong_credentials(self):
        """测试错误凭据"""
        with pytest.raises(Exception):
            client = NebulaClient(
                NEBULA_ADDRESS,
                "wrong_user",
                "wrong_password",
            )
            client.close()

    def test_connection_timeout(self):
        """测试连接超时"""
        from nebulagraph_python.client._connection import ConnectionConfig

        conn_config = ConnectionConfig.from_defaults(
            NEBULA_ADDRESS, connect_timeout=1.0
        )
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
            conn_config=conn_config,
        )
        # 应该能连接成功
        assert client.ping()
        client.close()


class TestPerformance:
    """性能测试"""

    def test_batch_insert(self):
        """测试批量插入"""
        client = NebulaClient(
            NEBULA_ADDRESS,
            NEBULA_USER,
            NEBULA_PASSWORD,
        )

        # 准备图空间
        client.execute("CREATE SPACE IF NOT EXISTS test_space(partition_num=10, replica_factor=1, vid_type=FIXED_STRING(32))")
        client.execute("USE test_space")
        client.execute("CREATE TAG IF NOT EXISTS person(name string, age int)")

        # 批量插入
        vertices = []
        for i in range(100):
            vertices.append(f'"{i}":("Person{i}", {20 + i % 30})')

        query = f'INSERT VERTEX person(name, age) VALUES {", ".join(vertices)}'
        result = client.execute(query)
        assert result.is_succeeded

        client.close()

    def test_concurrent_queries(self):
        """测试并发查询"""
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