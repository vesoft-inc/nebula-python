# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


"""
Nebula StorageClient tests.
"""

import pytest
import sys
import os
import time
import threading

from storage.ttypes import EntryId
from storage.ttypes import PropDef
from storage.ttypes import PropOwner
from storage.ttypes import ResultCode
from storage.ttypes import ErrorCode
from common.ttypes import HostAddr

sys.path.insert(0, '../')

from graph import ttypes
from nebula.ConnectionPool import ConnectionPool
from nebula.Client import GraphClient
from nebula.Common import *
from nebula.ngStorage.StorageClient import StorageClient
from nebula.ngMeta.MetaClient import MetaClient

def prepare():
    connection_pool = ConnectionPool(host, graph_port)
    client = GraphClient(connection_pool)
    if client.is_none():
        raise AuthException('Connect failed')
    resp = client.authenticate('user', 'password')
    assert resp.error_code == 0, resp.error_msg
    resp = client.execute('DROP SPACE IF EXISTS %s' % spaceName)
    assert resp.error_code == 0, resp.error_msg
    resp = client.execute('CREATE SPACE %s(partition_num=1)' % spaceName)
    assert resp.error_code == 0, resp.error_msg
    time.sleep(5)
    resp = client.execute('USE %s' % spaceName)
    assert resp.error_code == 0, resp.error_msg
    time.sleep(5)
    resp = client.execute('CREATE TAG player(name string, age int)')
    assert resp.error_code == 0, resp.error_msg
    resp = client.execute('CREATE EDGE follow(degree double)')
    assert resp.error_code == 0, resp.error_msg
    time.sleep(12)
    resp = client.execute('INSERT VERTEX player(name, age) VALUES 1:(\'Bob\', 18)')
    assert resp.error_code == 0, resp.error_msg
    resp = client.execute('INSERT VERTEX player(name, age) VALUES 2:(\'Tome\', 22)')
    assert resp.error_code == 0, resp.error_msg
    resp = client.execute('INSERT EDGE follow(degree) VALUES 1->2:(94.7)')
    assert resp.error_code == 0, resp.error_msg

def test_scanEdge():
    result = storageClient.scanEdge(spaceName, {'follow':['degree']}, True, 100, 0, sys.maxsize)
    assert result is not None and result.next().edge_data is not None

def test_scanVertex():
    result = storageClient.scanVertex(spaceName, {'player':['name', 'age']}, True, 100, 0, sys.maxsize)
    assert result is not None and result.next().vertex_data is not None

def test_scanPartEdge():
    result = storageClient.scanPartEdge(spaceName, 1, {'follow':['degree']}, True, 100, 0, sys.maxsize) 
    assert result is not None and result.next().edge_data is not None

def test_scanPartVertex():
    result = storageClient.scanPartVertex(spaceName, 1, {'player':['name', 'age']}, True, 100, 0, sys.maxsize)
    assert result is not None and result.next().vertex_data is not None

def test_getTagSchema():
    result = metaClient.getTagSchema(spaceName, 'player')
    expect = {'name':6, 'age':2}
    assert result == expect

def test_getEdgeSchema():
    result = metaClient.getEdgeSchema(spaceName, 'follow')
    expect = {'degree': 5}
    assert result == expect

def test_getEdgeReturnCols():
    edgeItem = metaClient.getEdgeItemFromCache(spaceName, 'follow')
    edgeType = edgeItem.edge_type
    entryId = EntryId(edge_type=edgeType)
    result = storageClient.getEdgeReturnCols(spaceName, {'follow':['degree']})
    expect = {edgeType:[PropDef(PropOwner.EDGE, entryId, 'degree')]}
    assert result == expect

def test_getVertexReturnCols():
    tagItem = metaClient.getTagItemFromCache(spaceName, 'player')
    tagId = tagItem.tag_id
    entryId = EntryId(tag_id=tagId)
    result = storageClient.getVertexReturnCols(spaceName, {'player':['name', 'age']})
    expect = {tagId:[PropDef(PropOwner.SOURCE, entryId, 'name'), PropDef(PropOwner.SOURCE, entryId, 'age')]}
    assert result == expect

def test_handleResultCodes():
    failedCodes = [ResultCode(code=ErrorCode.E_LEADER_CHANGED, part_id=1, leader=HostAddr(ip=2130706433, port=storage_port))]
    result, _ = storageClient.handleResultCodes(failedCodes, spaceName)
    expect = (host, storage_port)
    assert result == expect


host = '127.0.0.1'
meta_port = 45500
graph_port = 3699
storage_port = 44500
spaceName = 'test_storage'
prepare()
metaClient = MetaClient([('127.0.0.1', meta_port)])
metaClient.connect()
storageClient = StorageClient(metaClient)
