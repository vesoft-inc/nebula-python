#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from nebulagraph_python import NebulaPool, NebulaPoolConfig
from nebulagraph_python.client import NebulaBaseExecutor

class NebulaPoolExecutor(NebulaBaseExecutor):
    """Wrapper to make NebulaPool compatible with NebulaBaseExecutor"""

    def __init__(self, pool):
        self.pool = pool
        self.client = None

    def execute(self, statement: str, *, timeout=None, do_ping=False):
        if self.client is None:
            self.client = self.pool.get_client()
        return self.client.execute_with_timeout(statement, timeout or 30000)


graph_name = "test_graph"

def main():
    # configure the connection information
    addresses = "127.0.0.1:9669"
    user_name = "root"
    password = "NebulaGraph01"

    # create NebulaPool
    config = NebulaPoolConfig(
        addresses=addresses,
        user_name=user_name,
        password=password,
        graph=graph_name
    )
    pool = NebulaPool(config)

    try:
        print("use execute_py to execute `SHOW GRAPHS` ...")
        executor = NebulaPoolExecutor(pool)
        result = executor.execute("SHOW GRAPHS")

        # print results
        print("\n query result:")
        print("-" * 50)
        result.print()
        print("-" * 50)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        pool.close()


if __name__ == "__main__":
    main()