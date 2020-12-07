#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
import os


current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(current_dir, '..')
sys.path.insert(0, root_dir)

from nebula2.gclient.net import Connection
from nebula2.meta import ttypes
from nebula2.mclient import MetaCache


class TestMetaCache(object):
    @classmethod
    def setup_class(self):
        # create schema
        try:
            conn = Connection()
            conn.open('127.0.0.1', 3699, 1000)
            session_id = conn.authenticate('root', 'nebula')
            assert session_id != 0
            resp = conn.execute(session_id,
                                'CREATE SPACE IF NOT EXISTS test_meta_cache1(REPLICA_FACTOR=3);'
                                'USE test_meta_cache1;'
                                'CREATE TAG IF NOT EXISTS tag11(name string);'
                                'CREATE EDGE IF NOT EXISTS edge11(name string);'
                                'CREATE SPACE IF NOT EXISTS test_meta_cache2;'
                                'USE test_meta_cache2;'
                                'CREATE TAG IF NOT EXISTS tag22(name string);'
                                'CREATE EDGE IF NOT EXISTS edge22(name string);')
            assert resp.error_code == 0
            conn.close()
            self.meta_cache = MetaCache([('127.0.0.1', 45500),
                                         ('127.0.0.1', 45501),
                                         ('127.0.0.1', 45501)], 50000)
        except Exception as x:
            import traceback
            print(traceback.format_exc())
            assert False

    def test_get_space_id(self):
        space_id1 = self.meta_cache.get_space_id('test_meta_cache1')
        space_id2 = self.meta_cache.get_space_id('test_meta_cache2')
        assert 0 < space_id1 < space_id2

        # test not existed
        try:
            space_id = self.meta_cache.get_tag_id('test_meta_cache1', 'space_not_existed')
            assert False
        except Exception:
            assert True

    def test_get_tag_id(self):
        tag_id1 = self.meta_cache.get_tag_id('test_meta_cache1', 'tag11')
        tag_id2 = self.meta_cache.get_tag_id('test_meta_cache2', 'tag22')
        assert 0 < tag_id1 < tag_id2

        # test not existed
        try:
            tag_id = self.meta_cache.get_tag_id('test_meta_cache1', 'tag_not_existed')
            assert False
        except Exception:
            assert True

    def test_get_edge_type(self):
        edge_id1 = self.meta_cache.get_edge_type('test_meta_cache1', 'edge11')
        edge_id2 = self.meta_cache.get_edge_type('test_meta_cache2', 'edge22')
        assert 0 < edge_id1 < edge_id2

        # test not existed
        try:
            edge_id = self.meta_cache.get_edge_type('test_meta_cache1', 'edge_not_existed')
            assert False
        except Exception:
            assert True

    def test_get_tag_schema(self):
        tag_schema1 = self.meta_cache.get_tag_schema('test_meta_cache1', 'tag11')
        tag_schema2 = self.meta_cache.get_tag_schema('test_meta_cache2', 'tag22')
        assert tag_schema1.columns[0].name.decode('utf-8') == 'name'
        assert tag_schema1.columns[0].type.type == ttypes.PropertyType.STRING
        assert tag_schema1.columns[0].type.type_length == 0
        assert tag_schema2.columns[0].name.decode('utf-8') == 'name'
        assert tag_schema2.columns[0].type.type == ttypes.PropertyType.STRING
        assert tag_schema2.columns[0].type.type_length == 0

        # test not existed
        try:
            tag_item = self.meta_cache.get_tag_schema('test_meta_cache1', 'tag_not_existed')
            assert False
        except Exception:
            assert True

    def test_get_edge_schema(self):
        edge_schema1 = self.meta_cache.get_edge_schema('test_meta_cache1', 'edge11')
        edge_schema2 = self.meta_cache.get_edge_schema('test_meta_cache2', 'edge22')
        assert edge_schema1.columns[0].name.decode('utf-8') == 'name'
        assert edge_schema1.columns[0].type.type == ttypes.PropertyType.STRING
        assert edge_schema1.columns[0].type.type_length == 0
        assert edge_schema2.columns[0].name.decode('utf-8') == 'name'
        assert edge_schema2.columns[0].type.type == ttypes.PropertyType.STRING
        assert edge_schema2.columns[0].type.type_length == 0

        # test not existed
        try:
            edge_item = self.meta_cache.get_edge_schema('test_meta_cache1', 'edge_not_existed')
            assert False
        except Exception:
            assert True

    def test_get_part_leader(self):
        address = self.meta_cache.get_part_leader('test_meta_cache1', 1)
        assert address.host.find('storaged') == 0
        assert address.port == 44500

    def test_get_part_leaders(self):
        part_addresses = self.meta_cache.get_part_leaders('test_meta_cache1')

        parts = [part for part in part_addresses.keys()]
        assert len(parts) == 100
        expected_parts = [i for i in range(1, 101)]
        assert sorted(parts) == sorted(expected_parts)

        for part in part_addresses.keys():
            assert part_addresses[part].host in ['storaged0', 'storaged1', 'storaged2']

        ports = [part_addresses[part].port for part in part_addresses.keys()]
        expected_hosts = [44500 for i in range(1, 101)]
        assert ports == expected_hosts

    def test_get_all_storage_addrs(self):
        addresses = self.meta_cache.get_all_storage_addrs()
        assert len(addresses) == 3
        hosts = [addr.host for addr in addresses]
        expected_hosts = ['storaged0', 'storaged1', 'storaged2']
        hosts = sorted(hosts)
        expected_hosts = sorted(expected_hosts)
        assert hosts == expected_hosts

        ports = [addr.port for addr in addresses]
        expected_hosts = [44500, 44500, 44500]
        assert ports == expected_hosts


