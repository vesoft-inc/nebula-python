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
    logging.getLogger("nebulagraph_pyt[?1l>[?2004l
[1m[7m%[27m[1m[0m                                                                                                                                             ]2;nicole@nicole-2:~/workspace/nebula/nebula-python]1;..nebula-python]7;file://nicole-2/Users/nicole/workspace/nebula/nebula-python[0m[27m[24m[J[39m[0m[49m[40m[39m nicole@nicole-2 [44m[30m[30m ~/workspace/nebula/nebula-python [43m[34m[30m  improve_decode ±✚ [49m[33m[39m [K[?1h=[?2004h[32mg[39m[90mit st[39m[1m[31mg[1m[31mi[0m[39m[0m[32mg[0m[32mi[32mt[39m[39m [39m[4ms[24m[24ms[39mt[?1l>[?2004l
]2;git st]1;gitOn branch improve_decode
Changes to be committed:
  (use "git restore --staged <file>..." to unstage)
	[32mnew file:   scripts/decode_test.py[m

Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
	[31mmodified:   pdm.lock[m
	[31mmodified:   scripts/decode_test.py[m
	[31mmodified:   src/nebulagraph_python/decoder/data_types.py[m
	[31mmodified:   src/nebulagraph_python/decoder/decode.py[m
	[31mmodified:   src/nebulagraph_python/decoder/value_parser.py[m
	[31mmodified:   src/nebulagraph_python/py_data_types.py[m
	[31mmodified:   src/nebulagraph_python/result_set.py[m
	[31mmodified:   src/nebulagraph_python/value_wrapper.py[m

Untracked files:
  (use "git add <file>..." to include in what will be committed)
	[31mscripts/benchmark_decoder.py[m

[1m[7m%[27m[1m[0m                                                                                                                                             ]2;nicole@nicole-2:~/workspace/nebula/nebula-python]1;..nebula-python]7;file://nicole-2/Users/nicole/workspace/nebula/nebula-pythongi[0m[27m[24m[J[39m[0m[49m[40m[39m nicole@nicole-2 [44m[30m[30m ~/workspace/nebula/nebula-python [43m[34m[30m  improve_decode ±✚ [49m[33m[39m [K[?1h=[?2004hg[1m[31mg[1m[31mi[0m[39m[90mt st[39m[0m[32mg[0m[32mi[32mt[39m[39m [39ma[90mp[90mply /Users/nicole/Desktop/decoder_performance.patch[39m[52D[39md[90md[90m [90m.[39m[39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [39m [51D[39md[39m [39m[4m.[24m