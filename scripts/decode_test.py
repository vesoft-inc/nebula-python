from nebulagraph_python import NebulaClient, NebulaPool, NebulaPoolConfig
import time

def sync_client_example():
    """Example using synchronous NebulaClient"""

    # Create client using direct initialization
    # Note: NebulaClient automatically establishes connection in __init__
    # and inherits from NebulaBaseExecutor, so it can use execute_py() directly
    client = NebulaClient(
        addresses="192.168.8.6:3820",
        user_name="root",
        password="NebulaGraph01",
        connect_timeout_ms=30000,
        request_timeout_ms=300000
    )

    try:
        print(f"Connected to NebulaGraph at {client.get_host()}")
        print(f"Session ID: {client.get_session_id()}")
        print(f"Server version: {client.get_version()}")

        # Run benchmark query multiple times and report average timings.
        runs = 10
        total_latency_us = 0
        total_response_us = 0
        total_decode_us = 0

        for i in range(runs):
            start_us = time.time_ns() // 1000
            result = client.execute_with_timeout("use sf100_nicole match(v:Comment) return v  limit 10000", 50000)
            res_us = time.time_ns() // 1000
            for row in result:
                pass
            decode_us = time.time_ns() // 1000

            latency_us = result.latency_us
            response_us = res_us - start_us
            decode_cost_us = decode_us - res_us

            total_latency_us += latency_us
            total_response_us += response_us
            total_decode_us += decode_cost_us
            print(
                f"run {i + 1}/{runs} -> latency_us:{latency_us}, "
                f"response_us:{response_us}, decode_us:{decode_cost_us}"
            )

        avg_latency_us = total_latency_us / runs
        avg_response_us = total_response_us / runs
        avg_decode_us = total_decode_us / runs

        print(
            f"avg_latency_us:{avg_latency_us:.2f}, "
            f"avg_response_us:{avg_response_us:.2f}, "
            f"avg_decode_us:{avg_decode_us:.2f}"
        )


    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()
        print("Client closed\n")


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    sync_client_example()
