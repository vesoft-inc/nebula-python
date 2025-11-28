#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import random
import time

from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool
from nebula3.mclient import MetaCache
from nebula3.sclient.GraphStorageClient import GraphStorageClient


def prepare_data():
    config = Config()
    config.max_connection_pool_size = 1
    # init connection pool
    connection_pool = ConnectionPool()
    # the graphd server's address
    assert connection_pool.init([("127.0.0.1", 9671)], config)
    client = connection_pool.get_session("root", "nebula")
    client.execute(
        "CREATE SPACE IF NOT EXISTS ScanSpace("
        "PARTITION_NUM=10,"
        "vid_type=FIXED_STRING(20));"
        "USE ScanSpace;"
        "CREATE TAG IF NOT EXISTS person(name string, age int);"
        "CREATE EDGE IF NOT EXISTS friend(start int, end int);"
    )
    time.sleep(5)

    for id in range(20):
        vid = "person" + str(id)
        cmd = "INSERT VERTEX person(name, age) " 'VALUES "{}":("{}", {})'.format(
            vid, vid, id
        )
        client.execute(cmd)
    for id in range(20):
        src_id = "person" + str(id)
        dst_id = "person" + str(20 - id)
        start = random.randint(2000, 2010)
        end = random.randint(2010, 2020)
        cmd = "INSERT EDGE friend(start, end) " 'VALUES "{}"->"{}":({}, {})'.format(
            src_id, dst_id, start, end
        )
        client.execute(cmd)
    client.release()
    connection_pool.close()


def scan_person_vertex(graph_storage_client):
    resp = graph_storage_client.scan_vertex(
        space_name="ScanSpace", tag_name="person", limit=1
    )
    print("======== Scan vertexes in ScanSpace ======")
    while resp.has_next():
        result = resp.next()
        if result is not None:
            for vertex_data in result:
                print(vertex_data)


def scan_person_edge(graph_storage_client):
    resp = graph_storage_client.scan_edge(
        space_name="ScanSpace", edge_name="friend", limit=100
    )
    print("======== Scan edges in ScanSpace ======")
    while resp.has_next():
        result = resp.next()
        if result is not None:
            for edge_data in result:
                print(edge_data)


"""
The scan result
======== Scan vertexes in ScanSpace ======
('person11' :person{'name': "person11", 'age': 11})
('person16' :person{'name': "person16", 'age': 16})
('person9' :person{'name': "person9", 'age': 9})
('person10' :person{'name': "person10", 'age': 10})
('person15' :person{'name': "person15", 'age': 15})
('person0' :person{'name': "person0", 'age': 0})
('person2' :person{'name': "person2", 'age': 2})
('person13' :person{'name': "person13", 'age': 13})
('person18' :person{'name': "person18", 'age': 18})
('person6' :person{'name': "person6", 'age': 6})
('person7' :person{'name': "person7", 'age': 7})
('person12' :person{'name': "person12", 'age': 12})
('person17' :person{'name': "person17", 'age': 17})
('person5' :person{'name': "person5", 'age': 5})
('person8' :person{'name': "person8", 'age': 8})
('person4' :person{'name': "person4", 'age': 4})
('person1' :person{'name': "person1", 'age': 1})
('person14' :person{'name': "person14", 'age': 14})
('person19' :person{'name': "person19", 'age': 19})
('person3' :person{'name': "person3", 'age': 3})
======== Scan edges in ScanSpace ======
(person4)-[:friend@0{'start': 2000, 'end': 2015}]->(person16)
(person1)-[:friend@0{'start': 2002, 'end': 2020}]->(person19)
(person14)-[:friend@0{'start': 2008, 'end': 2020}]->(person6)
(person19)-[:friend@0{'start': 2009, 'end': 2013}]->(person1)
(person3)-[:friend@0{'start': 2010, 'end': 2011}]->(person17)
(person11)-[:friend@0{'start': 2001, 'end': 2017}]->(person9)
(person16)-[:friend@0{'start': 2007, 'end': 2014}]->(person4)
(person9)-[:friend@0{'start': 2001, 'end': 2017}]->(person11)
(person10)-[:friend@0{'start': 2009, 'end': 2020}]->(person10)
(person15)-[:friend@0{'start': 2002, 'end': 2018}]->(person5)
(person0)-[:friend@0{'start': 2008, 'end': 2017}]->(person20)
(person2)-[:friend@0{'start': 2009, 'end': 2012}]->(person18)
(person13)-[:friend@0{'start': 2003, 'end': 2012}]->(person7)
(person18)-[:friend@0{'start': 2004, 'end': 2012}]->(person2)
(person6)-[:friend@0{'start': 2001, 'end': 2017}]->(person14)
(person7)-[:friend@0{'start': 2009, 'end': 2015}]->(person13)
(person12)-[:friend@0{'start': 2007, 'end': 2010}]->(person8)
(person17)-[:friend@0{'start': 2008, 'end': 2013}]->(person3)
(person5)-[:friend@0{'start': 2005, 'end': 2015}]->(person15)
(person8)-[:friend@0{'start': 2000, 'end': 2019}]->(person12)
"""

if __name__ == "__main__":
    meta_cache = None
    graph_storage_client = None
    try:
        # the metad servers's address
        meta_cache = MetaCache(
            [("172.28.1.1", 9559), ("172.28.1.2", 9559), ("172.28.1.3", 9559)], 50000
        )
        graph_storage_client = GraphStorageClient(meta_cache)
        graph_storage_client.set_user_passwd("root", "nebula")
        prepare_data()
        scan_person_vertex(graph_storage_client)
        scan_person_edge(graph_storage_client)

    except Exception:
        import traceback

        print(traceback.format_exc())
        if graph_storage_client is not None:
            graph_storage_client.close()
        exit(1)
    finally:
        if graph_storage_client is not None:
            graph_storage_client.close()
        if meta_cache is not None:
            meta_cache.close()
