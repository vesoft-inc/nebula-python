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
import datetime

sys.path.insert(0, '../')
sys.path.insert(0, '../fbthrift')

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
from nebula.ngStorage.ngProcessor.ScanEdgeProcessor import ScanEdgeProcessor
from nebula.ngStorage.ngProcessor.ScanVertexProcessor import ScanVertexProcessor
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
        resp = client.execute('CREATE TAG team(name string, money double)')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('CREATE EDGE follow(degree double, likeness int)')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('CREATE EDGE serve(start timestamp, end timestamp)')
        assert resp.error_code == 0, resp.error_msg
        time.sleep(12)
        resp = client.execute('INSERT VERTEX player(name, age, married) VALUES 101:(\'Bob\', 18, FALSE)')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('INSERT VERTEX player(name, age, married) VALUES 102:(\'Tom\', 22, TRUE)')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('INSERT VERTEX player(name, age, married) VALUES 103:(\'Jerry\', 19, FALSE)')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('INSERT VERTEX team(name, money) VALUES 201:(\'Red Bull\', 185.85)')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('INSERT VERTEX team(name, money) VALUES 202:(\'River\', 567.93)')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('INSERT EDGE follow(degree, likeness) VALUES 101->102:(94.7, 45)')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('INSERT EDGE follow(degree, likeness) VALUES 102->103:(86.3, 79)')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('INSERT EDGE serve(start, end) VALUES 101->201:(\'2001-09-01 08:00:00\', \'2010-09-01 08:00:00\')')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('INSERT EDGE serve(start, end) VALUES 102->202:(\'1998-08-22 06:45:54\', \'2020-01-23 17:23:35\')')
        assert resp.error_code == 0, resp.error_msg
        resp = client.execute('INSERT EDGE serve(start, end) VALUES 103->201:(\'2006-11-18 13:28:29\', \'2009-12-12 12:21:46\')')
        assert resp.error_code == 0, resp.error_msg
    except Exception as ex:
        print(ex)
        client.sign_out()
        assert False

def check_result(result, expect):
    if len(result) != len(expect):
        print('len(result) != len(expect)')
        return False
    for name in expect.keys():
        if name not in result.keys():
            print(name, ' not in result')
            return False
        if len(result[name]) != len(expect[name]):
            print('len(result[%s]) != len(expect[%s])' % (name, name))
            return False
        for value in expect[name]:
            if value not in result[name]:
                print(value, ' not in result[%s]' % name)
                return False
    return True

def get_result(space, scan_response_iter, is_edge):
    result = {}
    while scan_response_iter.has_next():
        scan_response = scan_response_iter.next()
        if scan_response is None:
            assert False
        if is_edge:
            data = scan_edge_processor.process(space, scan_response)
        else:
            data = scan_vertex_processor.process(space, scan_response)
        for name, rows in data._rows.items():
            if name not in result.keys():
                result[name] = []
            for row in rows:
                props = {}
                for prop in row._default_properties:
                    props[prop.get_name()] = prop.get_value()
                for prop in row._properties:
                    props[prop.get_name()] = prop.get_value()
                result[name].append(props)
    return result

def ts(date_str):
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
    return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())

def test_scan_edge():
    scan_edge_response_iter = storage_client.scan_edge(space_name, {'follow': ['degree', 'likeness'], 'serve': ['start', 'end']}, True, 100, 0, sys.maxsize)
    result = get_result(space_name, scan_edge_response_iter, True)
    expect = {'follow': [{'_src': 101, '_edge': 'follow', '_dst': 102, 'degree': 94.7, 'likeness': 45},
                         {'_src': 102, '_edge': 'follow', '_dst': 103, 'degree': 86.3, 'likeness': 79}],
              'serve':  [{'_src': 101, '_edge': 'serve', '_dst': 201, 'start': ts('2001-09-01 08:00:00'), 'end': ts('2010-09-01 08:00:00')},
                         {'_src': 102, '_edge': 'serve', '_dst': 202, 'start': ts('1998-08-22 06:45:54'), 'end': ts('2020-01-23 17:23:35')},
                         {'_src': 103, '_edge': 'serve', '_dst': 201, 'start': ts('2006-11-18 13:28:29'), 'end': ts('2009-12-12 12:21:46')}]}
    assert check_result(result, expect)

def test_scan_vertex():
    scan_vertex_response_iter = storage_client.scan_vertex(space_name, {'player': ['name', 'age', 'married'], 'team': ['name', 'money']}, True, 100, 0, sys.maxsize)
    result = get_result(space_name, scan_vertex_response_iter, False)
    expect = {'player': [{'_vid': 101, '_tag': 'player', 'name': 'Bob', 'age': 18, 'married': False},
                         {'_vid': 102, '_tag': 'player', 'name': 'Tom', 'age': 22, 'married': True},
                         {'_vid': 103, '_tag': 'player', 'name': 'Jerry', 'age': 19, 'married': False}],
              'team': [{'_vid': 201, '_tag': 'team', 'name': 'Red Bull', 'money': 185.85},
                       {'_vid': 202, '_tag': 'team', 'name': 'River', 'money': 567.93}]}
    assert check_result(result, expect)

def test_scan_part_edge():
    scan_edge_response_iter = storage_client.scan_part_edge(space_name, 1, {'follow': ['degree', 'likeness'], 'serve': ['start', 'end']}, True, 100, 0, sys.maxsize)
    result = get_result(space_name, scan_edge_response_iter, True)
    expect = {'follow': [{'_src': 101, '_edge': 'follow', '_dst': 102, 'degree': 94.7, 'likeness': 45},
                         {'_src': 102, '_edge': 'follow', '_dst': 103, 'degree': 86.3, 'likeness': 79}],
              'serve':  [{'_src': 101, '_edge': 'serve', '_dst': 201, 'start': ts('2001-09-01 08:00:00'), 'end': ts('2010-09-01 08:00:00')},
                         {'_src': 102, '_edge': 'serve', '_dst': 202, 'start': ts('1998-08-22 06:45:54'), 'end': ts('2020-01-23 17:23:35')},
                         {'_src': 103, '_edge': 'serve', '_dst': 201, 'start': ts('2006-11-18 13:28:29'), 'end': ts('2009-12-12 12:21:46')}]}
    assert check_result(result, expect)

def test_scan_part_vertex():
    scan_vertex_response_iter = storage_client.scan_part_vertex(space_name, 1, {'player':['name', 'age', 'married'], 'team': ['name', 'money']}, True, 100, 0, sys.maxsize)
    result = get_result(space_name, scan_vertex_response_iter, False)
    expect = {'player': [{'_vid': 101, '_tag': 'player', 'name': 'Bob', 'age': 18, 'married': False},
                         {'_vid': 102, '_tag': 'player', 'name': 'Tom', 'age': 22, 'married': True},
                         {'_vid': 103, '_tag': 'player', 'name': 'Jerry', 'age': 19, 'married': False}],
              'team': [{'_vid': 201, '_tag': 'team', 'name': 'Red Bull', 'money': 185.85},
                       {'_vid': 202, '_tag': 'team', 'name': 'River', 'money': 567.93}]}
    assert check_result(result, expect)

def test_get_tag_schema():
    result = meta_client.get_tag_schema(space_name, 'player')
    expect = {'name': SupportedType.STRING, 'age': SupportedType.INT, 'married': SupportedType.BOOL}
    assert result == expect

    result = meta_client.get_tag_schema(space_name, 'team')
    expect = {'name': SupportedType.STRING, 'money': SupportedType.DOUBLE}
    assert result == expect

def test_get_edge_schema():
    result = meta_client.get_edge_schema(space_name, 'follow')
    expect = {'degree': SupportedType.DOUBLE, 'likeness': SupportedType.INT}
    assert result == expect

    result = meta_client.get_edge_schema(space_name, 'serve')
    expect = {'start': SupportedType.TIMESTAMP, 'end': SupportedType.TIMESTAMP}
    assert result == expect

def test_get_edge_return_cols():
    edge_item_1 = meta_client.get_edge_item_from_cache(space_name, 'follow')
    edge_type_1 = edge_item_1.edge_type
    entry_id_1 = EntryId(edge_type=edge_type_1)
    edge_item_2 = meta_client.get_edge_item_from_cache(space_name, 'serve')
    edge_type_2 = edge_item_2.edge_type
    entry_id_2 = EntryId(edge_type=edge_type_2)
    result = storage_client.get_edge_return_cols(space_name, {'follow': ['degree', 'likeness'], 'serve': ['start', 'end']})
    expect = {edge_type_1:[PropDef(PropOwner.EDGE, entry_id_1, 'degree'), PropDef(PropOwner.EDGE, entry_id_1, 'likeness')],
              edge_type_2:[PropDef(PropOwner.EDGE, entry_id_2, 'start'), PropDef(PropOwner.EDGE, entry_id_2, 'end')]}
    assert result == expect

def test_get_vertex_return_cols():
    tag_item_1 = meta_client.get_tag_item_from_cache(space_name, 'player')
    tag_id_1 = tag_item_1.tag_id
    entry_id_1 = EntryId(tag_id=tag_id_1)
    tag_item_2 = meta_client.get_tag_item_from_cache(space_name, 'team')
    tag_id_2 = tag_item_2.tag_id
    entry_id_2 = EntryId(tag_id=tag_id_2)
    result = storage_client.get_vertex_return_cols(space_name, {'player': ['name', 'age', 'married'], 'team': ['name', 'money']})
    expect = {tag_id_1:[PropDef(PropOwner.SOURCE, entry_id_1, 'name'), PropDef(PropOwner.SOURCE, entry_id_1, 'age'), PropDef(PropOwner.SOURCE, entry_id_1, 'married')],
              tag_id_2:[PropDef(PropOwner.SOURCE, entry_id_2, 'name'), PropDef(PropOwner.SOURCE, entry_id_2, 'money')]}
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
scan_edge_processor = ScanEdgeProcessor(meta_client)
scan_vertex_processor = ScanVertexProcessor(meta_client)
