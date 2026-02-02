import asyncio
from multiprocessing import Pool, cpu_count
from typing import List, Any
from nebulagraph_python import NebulaAsyncClient, SessionPoolConfig

async def worker_task(worker_id: int, query: str, hosts: List[str], username: str, password: str) -> dict:
    try:
        async with await NebulaAsyncClient.connect(
            hosts=hosts,
            username=username,
            password=password,
            session_pool_config=SessionPoolConfig(size=3),
        ) as client:
            result = await client.execute_py(query, {"worker_id": worker_id})
            return {
                "worker_id": worker_id,
                "status": "success",
                "result": list(result.as_primitive_by_row()),
            }
    except Exception as e:
        return {
            "worker_id": worker_id,
            "status": "error",
            "error": str(e),
        }

def run_async_in_process(worker_id: int, query: str, hosts: List[str], username: str, password: str) -> dict:
    return asyncio.run(worker_task(worker_id, query, hosts, username, password))


def multiprocess_with_different_queries():
    hosts = ["127.0.0.1:9669"]
    username = "root"
    password = "NebulaGraph01"

    tasks = [
        (0, "USE sf1 MATCH (v:Person) RETURN count(v) as person_count", hosts, username, password),
        (1, "USE sf1 MATCH (v:Comment) RETURN count(v) as comment_count", hosts, username, password),
        (2, "USE sf1 MATCH (v:Person)-[c:KNOWS]->(t:Person) RETURN count(c) as edge_num", hosts, username, password),
    ]

    with Pool(processes=len(tasks)) as pool:
        results = pool.starmap(run_async_in_process, tasks)

    for result in results:
        if result["status"] == "success":
            print(f"任务 {result['worker_id']}: {result['result']}")
        else:
            print(f"任务 {result['worker_id']}: 失败 - {result['error']}")



if __name__ == "__main__":
    multiprocess_with_different_queries()
