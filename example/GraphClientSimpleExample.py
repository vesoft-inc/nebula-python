#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import json
import time

from FormatResp import print_resp, result_to_df_buildin, result_to_df
import pandas as pd

from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool

if __name__ == "__main__":
    client = None
    try:
        config = Config()
        config.max_connection_pool_size = 2
        # init connection pool
        connection_pool = ConnectionPool()
        assert connection_pool.init([("127.0.0.1", 9669)], config)

        # get session from the pool
        client = connection_pool.get_session("root", "nebula")
        assert client is not None

        # get the result in json format
        resp_json = client.execute_json("yield 1")
        json_obj = json.loads(resp_json)
        print(json.dumps(json_obj, indent=2, sort_keys=True))

        client.execute(
            "CREATE SPACE IF NOT EXISTS test(vid_type=FIXED_STRING(30)); USE test;"
            "CREATE TAG IF NOT EXISTS person(name string, age int);"
            "CREATE EDGE IF NOT EXISTS like (likeness double);"
        )

        # insert data need to sleep after create schema
        time.sleep(6)

        # insert vertex
        resp = client.execute(
            'INSERT VERTEX person(name, age) VALUES "Bob":("Bob", 10), "Lily":("Lily", 9)'
        )
        assert resp.is_succeeded(), resp.error_msg()

        # insert edges
        resp = client.execute('INSERT EDGE like(likeness) VALUES "Bob"->"Lily":(80.0);')
        assert resp.is_succeeded(), resp.error_msg()

        resp = client.execute('FETCH PROP ON person "Bob" YIELD vertex as node')
        assert resp.is_succeeded(), resp.error_msg()
        print_resp(resp)

        resp = client.execute('FETCH PROP ON like "Bob"->"Lily" YIELD edge as e')
        assert resp.is_succeeded(), resp.error_msg()
        print_resp(resp)

        # query data
        resp = client.execute(
            'GET SUBGRAPH WITH PROP 2 STEPS FROM "Bob" YIELD VERTICES AS nodes, EDGES AS relationships;'
        )
        df = result_to_df_buildin(resp)
        df_1 = result_to_df(resp)

        print("Testing pandas dataframe operations")
        print(df_1)

        # Convert the dataframe 'df' into a CSV file
        df.to_csv('subgraph_data.csv', index=False)
        print("Dataframe 'df' has been exported to 'subgraph_data.csv'.")

        # Read the CSV file back into a dataframe
        df_csv = pd.read_csv('subgraph_data.csv')
        print("CSV file 'subgraph_data.csv' has been read into dataframe 'df_csv'.")

        # Display the first 5 rows of the dataframe
        print("Displaying the first 5 rows of dataframe 'df_csv':")
        print(df_csv.head())

        # drop space
        resp = client.execute("DROP SPACE test")
        assert resp.is_succeeded(), resp.error_msg()

        print("Example finished")

    except Exception:
        import traceback

        print(traceback.format_exc())
        if client is not None:
            client.release()
        exit(1)
