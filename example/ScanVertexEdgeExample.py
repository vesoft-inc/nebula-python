#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
import random
import sys
import time

sys.path.insert(0, '../')

from nebula2.Config import Config
from nebula2.gclient.net import ConnectionPool
from nebula2.mclient import MetaCache
from nebula2.sclient.GraphStorageClient import GraphStorageClient


def prepare_data():
    config = Config()
    config.max_connection_pool_size = 1
    # init connection pool
    connection_pool = ConnectionPool()
    assert connection_pool.init([('172.28.3.1', 3699)], config)
    client = connection_pool.get_session('root', 'nebula')
    client.execute('CREATE SPACE IF NOT EXISTS ScanSpace('
                   'PARTITION_NUM=10,'
                   'vid_type=FIXED_STRING(20));'
                   'USE ScanSpace;'
                   'CREATE TAG IF NOT EXISTS person(name string, age int);'
                   'CREATE EDGE IF NOT EXISTS friend(start int, end int);')
    time.sleep(5)

    for id in range(20):
        vid = 'person' + str(id)
        cmd = 'INSERT VERTEX person(name, age) ' \
              'VALUES \"{}\":(\"{}\", {})'.format(vid, vid, id)
        client.execute(cmd)
    for id in range(20):
        src_id = 'person' + str(id)
        dst_id = 'person' + str(20 - id)
        start = random.randint(2000, 2010)
        end = random.randint(2010, 2020)
        cmd = 'INSERT EDGE friend(start, end) ' \
              'VALUES \"{}\"->\"{}\":({}, {})'.format(src_id, dst_id, start, end)
        client.execute(cmd)
    client.release()
    connection_pool.close()


def scan_person_vertex(graph_storage_client):
    resp = graph_storage_client.scan_vertex(
        space_name='ScanSpace',
        tag_name='person',
        limit=100)
    print('======== Scan vertexes in ScanSpace ======')
    while resp.has_next():
        result = resp.next()
        for vertex_data in result:
            print(vertex_data)


def scan_person_edge(graph_storage_client):
    resp = graph_storage_client.scan_edge(
        space_name='ScanSpace',
        edge_name='friend',
        limit=100)
    print('======== Scan edges in ScanSpace ======')
    while resp.has_next():
        result = resp.next()
        for edge_data in result:
            print(edge_data)


if __name__ == '__main__':
    graph_storage_client = None
    try:
        meta_cache = MetaCache([('172.28.1.1', 45500),
                                ('172.28.1.2', 45500),
                                ('172.28.1.3', 45500)],
                               50000)
        graph_storage_client = GraphStorageClient(meta_cache)
        prepare_data()
        scan_person_vertex(graph_storage_client)
        scan_person_edge(graph_storage_client)

    except Exception as x:
        import traceback
        print(traceback.format_exc())
        if graph_storage_client is not None:
            graph_storage_client.close()
        exit(1)
