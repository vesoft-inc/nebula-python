#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Optional, Dict
from nebulagraph_python.client.pool import NebulaPool
from nebulagraph_python.data import HostAddress
from dataclasses import dataclass, field

@dataclass
class SessionConfig:
    schema: Optional[str] = None
    graph: Optional[str] = None
    timezone: Optional[str] = None
    values: Dict[str, str] = field(default_factory=dict)
    configs: Dict[str, str] = field(default_factory=dict)

graph_name = "test_graph"

def main():
    # config the connect information
    hosts = ["127.0.0.1:9669"]
    username = "root"
    password = "NebulaGraph01"

    # create NebulaPool
    pool = NebulaPool(
        hosts=hosts,
        username=username,
        password=password,
        session_config=SessionConfig(graph=graph_name)
    )

    try:
        print("use execute_py to execute `SHOW GRAPHS` ...")
        result = pool.execute_py("SHOW GRAPHS")

        # 打印结果
        print("\n query result:")
        print("-" * 50)
        result.print(style="table")

        print("\n\nuse execute to execute `SHOW GRAPHS`:")
        print("-" * 50)
        result2 = pool.execute("SHOW GRAPHS")
        result2.print(style="table")

        # get the query result
        print("-" * 50)
        if result.size > 0:
            for row in result:
                print(f"Row: {row}")
        else:
            print("Empty")

    except Exception as e:
        print(f"\nerror: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\nclose the pool...")
        pool.close()
        print("closed")


if __name__ == "__main__":
    main()
