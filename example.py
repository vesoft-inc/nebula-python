from nebulagraph_python import NebulaAsyncClient, SessionConfig, SessionPoolConfig


async def async_client_example():
    # Create client
    client = await NebulaAsyncClient.connect(
        hosts=["127.0.0.1:9669", "127.0.0.1:9670"],
        username="root",
        password="NebulaGraph01",
        session_config=SessionConfig(
            graph="movie",
            timezone="Asia/Shanghai",
            values={"a": "1", "b": "[1, 2, 3]"},
        ),
    )

    (await client.execute_py("RETURN $a, $b")).print()
    (await client.execute_py("SHOW CURRENT_SESSION")).print()
    (await client.execute_py("DESC GRAPH TYPE movie_type")).print()

    query = """
    USE movie
    MATCH p=(a:Movie{name:"Unpromised Land"})-[e:WithGenre]->(b:Genre) 
    RETURN p as path, e as edge_WithGenre, b as genre_node, a.name as movie_name, 3.14 as float_val, true as bool_val
    LIMIT 2
    """
    # Execute query
    result = await client.execute(query)

    # Print results
    result.print()

    # Convert to pandas DataFrame
    # df = result.as_pandas_df()
    # df.to_csv("query_result.csv", index=False)

    # Get one row
    result = await client.execute(query)
    row = result.one()

    # Get column names
    print(row.column_names)

    # Get one value
    cell = row.col_values[0].cast()
    print(type(cell), cell)

    # Cast to primitive
    cell_primitive = row.col_values[0].cast_primitive()
    print(type(cell_primitive), cell_primitive)

    ######
    # special value type example
    ######

    query = """
    RETURN local_datetime("2016-09-20T01:01:01", "%Y-%m-%dT%H:%M:%S") AS localdatetime,
           local_time("05:06:07.089", "%H:%M:%S") AS localtime,
           zoned_time("05:06:07.089 +08:00", "%H:%M:%S %Ez") AS zonetime,
           zoned_datetime("2016-09-20T01:01:01 +0800", "%Y-%m-%dT%H:%M:%S %z") AS zoneddatetime,
           date("Tue, 2016-09-20", "%a, %Y-%m-%d") AS d,
           RECORD {a: 1, b: true, c: "str literal"} AS record1,
           LIST [1, 2, 3, 4, 5] AS l,
           "str literal" AS str_literal
    """

    (await client.execute_py(query)).print()

    ######
    # execute_py example
    ######

    query = """
    RETURN {{v1}} as v1, {{v2}} as v2, {{v3}} as v3
    """
    args = {"v1": 1, "v2": "alice", "v3": [True, False, True]}

    res = await client.execute_py(query, args)
    # get the first row in primitive type
    row = res.one().as_primitive()
    res.print()
    # assert the row is the same as the args, in python primitive type
    assert row == args
    # get the result in column-oriented primitive type
    print(res.as_primitive_by_column())
    # get the result in row-oriented primitive type
    print(list(res.as_primitive_by_row()))

    ######
    # embedding vector example
    ######

    # FOR DDL, DML refer to ann.feature

    # Query KNN
    await client.execute_py(
        """
    CREATE GRAPH TYPE  IF NOT EXISTS ann_test_type {
        NODE N1 (:N1&N2{
            idx INT64 PRIMARY KEY,
            vec1 VECTOR<3, FLOAT>
        })
    }
    """
    )

    await client.execute_py(
        """
    CREATE GRAPH IF NOT EXISTS ann_test ann_test_type 
    """
    )

    await client.execute_py(
        """
    USE ann_test INSERT OR REPLACE (@N1 {idx: 1, vec1: vector<3, float>([1, 2, 3])})
    """
    )

    await client.execute_py(
        """
    USE ann_test INSERT OR REPLACE (@N1 {idx: 2, vec1: vector<3, float>([4, 5, 6])})
    """
    )

    query = """
    USE ann_test
    MATCH (v:N1|N2)
    ORDER BY vector_distance(vector<3, float>([1, 2, 3]), v.vec1) LIMIT 3
    RETURN v, v.vec1 as vec1
    """

    (await client.execute_py(query)).print()

    await client.close()  # Explicitly close the client to release all resources


async def async_session_pool_example():
    """In this example we will create a client with session pool to execute queries async concurrently"""
    from asyncio import gather

    client = await NebulaAsyncClient.connect(
        hosts=["127.0.0.1:9669", "127.0.0.1:9670"],
        username="root",
        password="NebulaGraph01",
        session_pool_config=SessionPoolConfig(),  # Add the session pool config to use session pool
    )

    tasks = [client.execute_py("RETURN {{idx}}", {"idx": x}) for x in range(8)]
    results = await gather(*tasks)
    for result in results:
        print(result.as_primitive_by_column())
    await client.close()  # Explicitly close to release all resources

    # Using context manager to automatically close the client
    async with await NebulaAsyncClient.connect(
        hosts=["127.0.0.1:9669"],
        username="root",
        password="NebulaGraph01",
    ) as client:
        (await client.execute_py("RETURN 1")).print()


def sync_session_pool_example():
    """In this example we will create a client with session pool to execute queries multi-threaded concurrently"""
    from concurrent.futures import ThreadPoolExecutor

    from nebulagraph_python import NebulaClient, SessionPoolConfig

    # Using context manager to automatically close the client
    with (
        NebulaClient(
            hosts=["127.0.0.1:9669"],
            username="root",
            password="NebulaGraph01",
            session_pool_config=SessionPoolConfig(),  # Add the session pool config to use session pool
        ) as client,
        ThreadPoolExecutor(max_workers=8) as executor,
    ):
        futures = [
            executor.submit(client.execute_py, "RETURN {{idx}}", {"idx": x})
            for x in range(8)
        ]
        for future in futures:
            print(future.result().as_primitive_by_column())


if __name__ == "__main__":
    import asyncio
    import logging
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("nebulagraph_python").setLevel(logging.DEBUG)

    asyncio.run(async_client_example())
    asyncio.run(async_session_pool_example())
    sync_session_pool_example()
