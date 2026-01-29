#!/usr/bin/env python3
"""
测试重构后的NebulaClient和NebulaPool
"""

from nebulagraph_python.client import NebulaClient, NebulaPool, NebulaPoolConfig

print("=" * 60)
print("测试重构后的NebulaClient")
print("=" * 60)

try:
    # 测试NebulaClient
    client = NebulaClient(
        "192.168.8.6:3820",
        "root",
        "NebulaGraph01",
        connect_timeout_ms=3000,
        request_timeout_ms=60000,
    )

    print(f"✓ 成功创建NebulaClient")
    print(f"  Session ID: {client.get_session_id()}")
    print(f"  Version: {client.get_version()}")
    print(f"  Host: {client.get_host()}")

    # 执行查询
    result = client.execute("SHOW GRAPHS")
    print(f"✓ 成功执行查询: SHOW GRAPHS")
    print(f"  结果: {result}")

    # 测试ping
    ping_result = client.ping()
    print(f"✓ Ping结果: {ping_result}")

    client.close()
    print("✓ 客户端已关闭")

except Exception as e:
    print(f"✗ NebulaClient测试失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("测试重构后的NebulaPool")
print("=" * 60)

try:
    # 测试NebulaPool
    config = NebulaPoolConfig(
        addresses="192.168.8.6:3820",
        username="root",
        password="NebulaGraph01",
        max_client_size=3,
        min_client_size=1,
    )

    pool = NebulaPool(config)
    print(f"✓ 成功创建NebulaPool")

    # 通过pool获取client执行查询
    client = pool.get_client()
    result = client.execute("SHOW GRAPHS")
    print(f"✓ 成功执行查询: SHOW GRAPHS")
    print(f"  结果: {result}")
    pool.return_client(client)

    # 测试获取和返回客户端
    client1 = pool.get_client()
    print(f"✓ 成功获取客户端1")
    print(f"  活跃会话数: {pool.get_active_sessions()}")
    print(f"  空闲会话数: {pool.get_idle_sessions()}")

    client2 = pool.get_client()
    print(f"✓ 成功获取客户端2")
    print(f"  活跃会话数: {pool.get_active_sessions()}")

    pool.return_client(client1)
    print(f"✓ 成功返回客户端1")
    print(f"  活跃会话数: {pool.get_active_sessions()}")
    print(f"  空闲会话数: {pool.get_idle_sessions()}")

    pool.return_client(client2)
    print(f"✓ 成功返回客户端2")

    pool.close()
    print("✓ 连接池已关闭")

except Exception as e:
    print(f"✗ NebulaPool测试失败: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
print("测试完成")
print("=" * 60)