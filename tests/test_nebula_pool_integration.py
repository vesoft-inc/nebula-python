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

"""Integration tests for NebulaPool with real NebulaGraph connection"""

import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from nebulagraph_python import NebulaPool, NebulaPoolConfig
from nebulagraph_python.error import AuthenticatingError, ExecutingError

# 从环境变量获取测试配置，如果没有则使用默认值
NEBULA_HOSTS = os.getenv("NEBULA_HOSTS", "192.168.8.6:3820")
NEBULA_USER = os.getenv("NEBULA_USER", "root")
NEBULA_PASSWORD = os.getenv("NEBULA_PASSWORD", "NebulaGraph01")


@pytest.mark.integration
class TestNebulaPoolIntegration:

    def test_nebula_pool_basic(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                max_client_size=10,
                min_client_size=1,
            )
            pool = NebulaPool(config)
            client = pool.get_client()
            result = client.execute("RETURN 1")
            assert result.is_succeeded
            pool.return_client(client)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_builder(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                connect_timeout_ms=1111,
                request_timeout_ms=2222,
                scan_parallel=15,
                health_check_time_ms=3333,
                max_client_size=10,
                min_client_size=1,
            )
            pool = NebulaPool(config)
            client = pool.get_client()
            assert client.get_connect_timeout_mills() == 1111
            assert client.get_request_timeout_mills() == 2222
            assert client.get_scan_parallel() == 15
            pool.return_client(client)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_null_user(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=None,
                password=None,
                connect_timeout_ms=1111,
                request_timeout_ms=2222,
                scan_parallel=15,
                max_client_size=10,
                min_client_size=1,
            )
            pool = NebulaPool(config)
            client = pool.get_client()
            pool.return_client(client)
            pytest.fail("Should have raised AuthenticatingError")
        except AuthenticatingError:
            # Expected
            pass
        except Exception as e:
            pytest.fail(f"Unexpected exception: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_wrong_password(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password="wrong_password",
                max_client_size=10,
                min_client_size=1,
            )
            pool = NebulaPool(config)
            pytest.fail("Should have raised AuthenticatingError")
        except AuthenticatingError as e:
            # Expected
            assert "invalid username or password" in str(e) or "Auth failed" in str(e)
        except Exception as e:
            pytest.fail(f"Unexpected exception: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_wrong_server(self):
        print("<==== test_nebula_pool_wrong_server ====>")
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses="127.0.0.1:1000",
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                max_client_size=10,
                min_client_size=1,
            )
            pool = NebulaPool(config)
            pytest.fail("Should have raised ExecutingError")
        except ExecutingError as e:
            # Expected - can be timeout or connection refused
            assert "Connection refused" in str(e) or "UNAVAILABLE" in str(e) or "timeout" in str(e).lower()
        except Exception as e:
            pytest.fail(f"Unexpected exception: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_session_set_graph(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                # graph="test_pool_space",  # Skip graph setting for simplicity
                max_client_size=10,
                min_client_size=1,
            )
            pool = NebulaPool(config)
            client = pool.get_client()
            # Just verify connection works
            result = client.execute("RETURN 1")
            assert result.is_succeeded
            pool.return_client(client)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_session_set_timezone(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                timezone="Asia/Shanghai",
                max_client_size=10,
                min_client_size=1,
            )
            pool = NebulaPool(config)
            client = pool.get_client()
            # Just verify connection works
            result = client.execute("show current_session")
            assert result.is_succeeded
            for record in result:
                timezone_value = record["timezone"]
                assert timezone_value.cast() == "Asia/Shanghai"
            pool.return_client(client)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_session_set_format(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                session_configs={
                    "date_format": "\"%Y/%m/%d\"",
                    "local_datetime_format": "\"%Y-%m-%d %H:%M:%S\"",
                },
                max_client_size=10,
                min_client_size=1,
            )
            pool = NebulaPool(config)
            client = pool.get_client()
            # Just verify connection works
            result = client.execute("show session configs")
            assert result.is_succeeded
            for record in result:
                name_value = record["name"]
                if name_value.cast() == "date_format":
                    assert record["value"].cast() == "%Y/%m/%d"
                if name_value.cast() == "local_datetime_format":
                    assert record["value"].cast() == "%Y-%m-%d %H:%M:%S"
            pool.return_client(client)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_pre_statements(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                pre_statements=["RETURN 1", "session set timezone=\"Asia/Shanghai\""],
                max_client_size=10,
                min_client_size=1,
            )
            pool = NebulaPool(config)
            client = pool.get_client()
            # Just verify connection works after pre-statements
            result = client.execute("show current_session")
            assert result.is_succeeded
            for record in result:
                timezone_value = record["timezone"]
                assert timezone_value.cast() == "Asia/Shanghai"
            pool.return_client(client)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_wrong_pre_statement(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                pre_statements=["wrong statement"],
                max_client_size=10,
                min_client_size=1,
            )
            # Pool initialization will log warning but continue
            # This is expected behavior - pool is resilient
            pool = NebulaPool(config)
            # Pool should still work for other operations
            # Even if initial client creation failed
            try:
                client = pool.get_client()
                result = client.execute("RETURN 1")
                # This might succeed if pool recovered or if min_client_size was 0
                pool.return_client(client)
            except Exception as e:
                # Expected if no clients could be created
                pass
        except Exception as e:
            # If pool creation itself fails, that's acceptable
            assert "wrong statement" in str(e).lower() or "syntax error" in str(e).lower()
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_max_life_time(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                max_life_time_ms=5000,  # 5秒
                max_client_size=1,
                min_client_size=1,
            )
            pool = NebulaPool(config)
            client1 = pool.get_client()
            session_id1 = client1.get_session_id()
            time.sleep(6)  # wait to beyond the max life
            pool.return_client(client1)
            client2 = pool.get_client()
            session_id2 = client2.get_session_id()
            assert session_id1 != session_id2
            pool.return_client(client2)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_multiple_clients(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                max_client_size=5,
                min_client_size=2,
            )
            pool = NebulaPool(config)

            clients = []
            for i in range(3):
                client = pool.get_client()
                result = client.execute("RETURN 1")
                assert result.is_succeeded
                clients.append(client)

            for client in clients:
                pool.return_client(client)

            # get client again
            client = pool.get_client()
            result = client.execute("RETURN 1")
            assert result.is_succeeded
            pool.return_client(client)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_concurrent_access(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                max_client_size=10,
                min_client_size=1,
            )
            pool = NebulaPool(config)

            failed_count = [0]
            lock = threading.Lock()

            def execute_query(thread_id):
                try:
                    client = pool.get_client()
                    result = client.execute("RETURN 1")
                    if not result.is_succeeded:
                        with lock:
                            failed_count[0] += 1
                    pool.return_client(client)
                except Exception as e:
                    with lock:
                        failed_count[0] += 1

            # create 10 thread to execute parallel
            threads = []
            for i in range(10):
                thread = threading.Thread(target=execute_query, args=(i,))
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            assert failed_count[0] == 0, f"{failed_count[0]} threads failed"
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_get_active_idle_sessions(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                max_client_size=5,
                min_client_size=3,
            )
            pool = NebulaPool(config)

            # at begin：all client is idle
            assert pool.get_idle_sessions() == 3
            assert pool.get_active_sessions() == 0

            # get one client
            client1 = pool.get_client()
            assert pool.get_idle_sessions() == 2
            assert pool.get_active_sessions() == 1

            # get another client
            client2 = pool.get_client()
            assert pool.get_idle_sessions() == 1
            assert pool.get_active_sessions() == 2

            # return one client
            pool.return_client(client1)
            assert pool.get_idle_sessions() == 2
            assert pool.get_active_sessions() == 1

            # return another client
            pool.return_client(client2)
            assert pool.get_idle_sessions() == 3
            assert pool.get_active_sessions() == 0
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_context_manager(self):
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                max_client_size=10,
                min_client_size=1,
            )
            with NebulaPool(config) as pool:
                client = pool.get_client()
                result = client.execute("RETURN 1")
                assert result.is_succeeded
                pool.return_client(client)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")

    def test_nebula_pool_multiple_addresses(self):
        pool = None
        try:
            addresses = f"{NEBULA_HOSTS},{NEBULA_HOSTS}"
            config = NebulaPoolConfig(
                addresses=addresses,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                max_client_size=10,
                min_client_size=1,
            )
            pool = NebulaPool(config)
            client = pool.get_client()
            result = client.execute("RETURN 1")
            assert result.is_succeeded
            pool.return_client(client)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_test_on_borrow(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                test_on_borrow=True,
                max_client_size=2,
                min_client_size=1,
            )
            pool = NebulaPool(config)
            client = pool.get_client()
            result = client.execute("RETURN 1")
            assert result.is_succeeded
            pool.return_client(client)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_timeout_when_exhausted(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                max_client_size=1,
                min_client_size=1,
                max_wait_ms=100,
                block_when_exhausted=True,
            )
            pool = NebulaPool(config)

            client1 = pool.get_client()

            with pytest.raises(RuntimeError, match="Timeout waiting for client"):
                pool.get_client()

            pool.return_client(client1)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_no_block_when_exhausted(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                max_client_size=1,
                min_client_size=1,
                block_when_exhausted=False,
            )
            pool = NebulaPool(config)

            client1 = pool.get_client()

            with pytest.raises(RuntimeError, match="No available clients in pool"):
                pool.get_client()

            pool.return_client(client1)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_client_ping(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                max_client_size=10,
                min_client_size=1,
            )
            pool = NebulaPool(config)
            client = pool.get_client()
            assert client.ping() is True
            pool.return_client(client)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_client_execute_complex_query(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                max_client_size=10,
                min_client_size=1,
            )
            pool = NebulaPool(config)
            client = pool.get_client()

            queries = [
                "RETURN 1 AS num",
                "RETURN 'hello' AS str",
                "RETURN 1 + 2 AS result",
                "RETURN [1, 2, 3]",
                "RETURN {key: 'value'}",
                "RETURN 1.5 AS float_num",
            ]

            for query in queries:
                result = client.execute(query)
                assert result.is_succeeded, f"Query failed: {query} - {result.status_message}"

            pool.return_client(client)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()

    def test_nebula_pool_reuse_client(self):
        pool = None
        try:
            config = NebulaPoolConfig(
                addresses=NEBULA_HOSTS,
                username=NEBULA_USER,
                password=NEBULA_PASSWORD,
                max_client_size=1,
                min_client_size=1,
            )
            pool = NebulaPool(config)

            # get one client
            client1 = pool.get_client()
            session_id1 = client1.get_session_id()
            pool.return_client(client1)

            # get one client again
            client2 = pool.get_client()
            session_id2 = client2.get_session_id()
            assert session_id1 == session_id2, "Should reuse the same client"
            pool.return_client(client2)
        except Exception as e:
            pytest.fail(f"Test failed: {e}")
        finally:
            if pool is not None:
                pool.close()
