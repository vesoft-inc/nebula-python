#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2024 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import json


from nebula3.gclient.net import ConnectionPool


def get_node_list_and_edge_list_json(result):
    # init connection pool
    connection_pool = ConnectionPool()
    assert connection_pool.init([("127.0.0.1", 9669)])

    # get session from the pool
    client = connection_pool.get_session("root", "nebula")
    assert client is not None

    client.execute("USE nba")

    result = client.execute(
        'GET SUBGRAPH WITH PROP 2 STEPS FROM "player101" YIELD VERTICES AS nodes, EDGES AS relationships;'
    )

    assert result.is_succeeded(), result.error_msg()

    data = result.dict_for_vis()

    json_data = json.dumps(data, indent=2, sort_keys=True)

    # save the json data to a file
    with open('data.json', 'w') as f:
        f.write(json_data)

    # Check the data.json file to see the result

    # See example/apache_echarts.html to see a reference implementation of the visualization
    # using Apache ECharts
