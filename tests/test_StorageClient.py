# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


"""
Nebula storage_client tests.
"""

import pytest
import sys
import os
import time
import threading

sys.path.insert(0, '../')
from storage.ttypes import EntryId
from storage.ttypes import PropDef
from storage.ttypes import PropOwner
from storage.ttypes import ResultCode
from storage.ttypes import ErrorCode
from common.ttypes import HostAddr
from common.ttypes import SupportedType
from graph import ttypes
from nebula.ConnectionPool import ConnectionPool
from nebula.Client import GraphClient
from nebula.Common import *
from nebula.ngStorage.StorageClient import StorageClient
from nebula.ngMeta.MetaClient import MetaClient


def test_prepare():
    try:
        client = GraphClient(ConnectionPool(host, graph_port))
        if client is None:
            print('Error: None GraphClient')
            assert False
            return
        resp = client.authenticate('user', 'password')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('DROP SPACE IF EXISTS %s' % space_name)
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('CREATE SPACE %s(partition_num=1)' % space_name)
        assert resp.error_code == 0, resp.error_msg
        time.sleep(5)
        resp = client.execute('USE %s' % space_name)
        assert resp.error_code == 0, resp.error_msg
        time.sleep(5)
        resp = client.execute('CREATE TAG player(name string, age int, married bool)')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('CREATE EDGE follow(degree double, time timestamp)')
        assert resp.error_code == 0, resp.error_msg
        time.sleep(12)
        resp = client.execute('INSERT VERTEX player(name, age, married) VALUES 1:(\'Bob\', 18, FALSE)')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('INSERT VERTEX player(name, age, married) VALUES 2:(\'Tome\', 22, TRUE)')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('INSERT EDGE follow(degree, time) VALUES 1->2:(94.7, \'2010-09-01 08:00:00\')')
        assert resp.error_code == 0, resp.error_msg
    except Exception as ex:
        print(ex)
        client.sign_out()
        assert False

def test_scan_edge():
    result = storage_client.scan_edge(space_name, {'follow':['degree', 'time']}, True, 100, 0, sys.maxsize)
    assert result is not None and result.next().edge_data is not None

def test_scan_vertex():
    result = storage_client.scan_vertex(space_name, {'player':['name', 'age', 'married']}, True, 100, 0, sys.maxsize)
    assert result is not None and result.next().vertex_data is not None

def test_scan_part_edge():
    result = storage_client.scan_part_edge(space_name, 1, {'follow':['degree', 'time']}, True, 100, 0, sys.maxsize)
    assert result is not None and result.next().edge_data is not None

def test_scan_part_vertex():
    result = storage_client.scan_part_vertex(space_name, 1, {'player':['name', 'age', 'married']}, True, 100, 0, sys.maxsize)
    assert result is not None and result.next().vertex_data is not None

def test_get_tag_schema():
    result = meta_client.get_tag_schema(space_name, 'player')
    expect = {'name': SupportedType.STRING, 'age': SupportedType.INT, 'married': SupportedType.BOOL}
    assert result == expect

def test_get_edge_schema():
    result = meta_client.get_edge_schema(space_name, 'follow')
    expect = {'degree': SupportedType.DOUBLE, 'time': SupportedType.TIMESTAMP}
    assert result == expect

def test_get_edge_return_cols():
    edge_item = meta_client.get_edge_item_from_cache(space_name, 'follow')
    edge_type = edge_item.edge_type
    entry_id = EntryId(edge_type=edge_type)
    result = storage_client.get_edge_return_cols(space_name, {'follow':['degree', 'time']})
    expect = {edge_type:[PropDef(PropOwner.EDGE, entry_id, 'degree'), PropDef(PropOwner.EDGE, entry_id, 'time')]}
    assert result == expect

def test_get_vertex_return_cols():
    tag_item = meta_client.get_tag_item_from_cache(space_name, 'player')
    tag_id = tag_item.tag_id
    entry_id = EntryId(tag_id=tag_id)
    result = storage_client.get_vertex_return_cols(space_name, {'player':['name', 'age', 'married']})
    expect = {tag_id:[PropDef(PropOwner.SOURCE, entry_id, 'name'), PropDef(PropOwner.SOURCE, entry_id, 'age'), PropDef(PropOwner.SOURCE, entry_id, 'married')]}
    assert result == expect

def test_handle_result_codes():
    failed_codes = [ResultCode(code=ErrorCode.E_LEADER_CHANGED, part_id=1, leader=HostAddr(ip=2130706433, port=storage_port))]
    result, _ = storage_client.handle_result_codes(failed_codes, space_name)
    expect = (host, storage_port)
    assert result == expect


host = '127.0.0.1'
meta_port = 45500
graph_port = 3699
storage_port = 44500
space_name = 'test_storage'
meta_client = MetaClient([(host, meta_port)])
meta_client.connect()
storage_client = StorageClient(meta_client)
