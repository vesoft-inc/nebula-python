#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import logging
import os
import random
import time

import pytest

from nebula3.Exception import (
    EdgeNotFoundException,
    PartNotFoundException,
    SpaceNotFoundException,
    TagNotFoundException,
)
from nebula3.gclient.net import Connection
from nebula3.mclient import MetaCache
from nebula3.sclient.BaseResult import EdgeData
from nebula3.sclient.GraphStorageClient import GraphStorageClient
from nebula3.sclient.ScanResult import VertexData

logging.basicConfig(level=logging.INFO, format="[%(asctime)s]:%(message)s")


class TestGraphStorageClient(object):
    @staticmethod
    def execute_with_retry(conn, session_id, stmt, retry=3):
        count = retry
        while count > 0:
            count -= 1
            resp = conn.execute(session_id, stmt)
            if resp.error_code == 0:
                return
        assert False, "Execute `{}` failed: {}".format(
            stmt, resp.error_msg.decode("utf-8")
        )

    @classmethod
    def setup_class(cls):
        try:
            conn = Connection()
            conn.open("127.0.0.1", 9671, 3000)

            auth_result = conn.authenticate("root", "nebula")
            session_id = auth_result.get_session_id()
            assert session_id != 0
            cls.execute_with_retry(
                conn,
                session_id,
                "CREATE SPACE IF NOT EXISTS test_graph_storage_client("
                "PARTITION_NUM=10,"
                "REPLICA_FACTOR=1,"
                "vid_type=FIXED_STRING(20));"
                "USE test_graph_storage_client;"
                "CREATE TAG IF NOT EXISTS person(name string, age int);"
                "CREATE EDGE IF NOT EXISTS friend(start int, end int);",
            )
            time.sleep(5)

            for id in range(1000):
                vid = "person" + str(id)
                cmd = (
                    "INSERT VERTEX person(name, age) "
                    'VALUES "{}":("{}", {})'.format(vid, vid, id)
                )
                cls.execute_with_retry(conn, session_id, cmd)
            for id in range(1000):
                src_id = "person" + str(id)
                dst_id = "person" + str(1000 - id)
                start = random.randint(2000, 2010)
                end = random.randint(2010, 2020)
                cmd = (
                    "INSERT EDGE friend(start, end) "
                    'VALUES "{}"->"{}":({}, {})'.format(src_id, dst_id, start, end)
                )
                cls.execute_with_retry(conn, session_id, cmd)
            conn.close()

            meta_cache = MetaCache(
                [("172.28.1.1", 9559), ("172.28.1.2", 9559), ("172.28.1.3", 9559)],
                50000,
            )
            cls.graph_storage_client = GraphStorageClient(meta_cache)

        except Exception:
            import traceback

            print(traceback.format_exc())
            assert False

    def test_scan_tag_with_no_existed_space_name(self):
        try:
            self.graph_storage_client.scan_vertex(
                space_name="not_existed", tag_name="person", limit=1
            )
            assert False
        except SpaceNotFoundException:
            assert True
        except Exception as e:
            assert False, e

    def test_scan_tag_with_no_existed_part_id(self):
        try:
            self.graph_storage_client.scan_vertex_with_part(
                space_name="test_graph_storage_client",
                part=3000,
                tag_name="person",
                limit=1,
            )
            assert False
        except PartNotFoundException:
            assert True
        except Exception as e:
            assert False, e

    def test_scan_vertex_data(self):
        resp = self.graph_storage_client.scan_vertex(
            space_name="test_graph_storage_client", tag_name="person", limit=1
        )
        assert resp.has_next()
        result = resp.next()
        # test get_data_set
        data_set = result.get_data_set_wrapper()
        assert data_set.get_row_size() == 10
        assert data_set.get_col_names() == [
            "_vid",
            "person._vid",
            "person.name",
            "person.age",
        ]

        # test as nodes
        assert len(result.as_nodes()) >= 10

        # test iterator and VertexData
        count = 0
        for vertex in result:
            count += 1
            assert isinstance(vertex, VertexData)
            assert vertex.get_id().as_string().find("person") >= 0
            # test get_prop_values
            prop_values = vertex.get_prop_values()
            assert len(prop_values) == 2
            assert prop_values[0].is_string()
            assert prop_values[0].as_string().find("person") >= 0
            assert prop_values[1].is_int()
            assert prop_values[1].as_int() >= 0

            # test as node
            node = vertex.as_node()
            assert node.prop_names("person") == ["name", "age"]
            assert node.tags() == ["person"]
            assert node.get_id().as_string().find("person") >= 0
        assert count > 1

    @pytest.mark.skip(reason="cannot test with next cursor yet")
    def test_scan_vertex(self):
        # test get all by once
        resp = self.graph_storage_client.scan_vertex(
            space_name="test_graph_storage_client", tag_name="person", limit=2000
        )
        next_count = 0
        result1 = []
        while resp.has_next():
            next_count += 1
            result = resp.next()
            data_set = result.get_data_set_wrapper()
            assert data_set.get_row_size() == 1000
            result1.extend(data_set)
        assert next_count == 1

        # test with cursor
        resp = self.graph_storage_client.scan_vertex(
            space_name="test_graph_storage_client", tag_name="person", limit=10
        )
        next_count = 0
        result2 = []
        while resp.has_next():
            next_count += 1
            result = resp.next()
            data_set = result.get_data_set_wrapper()
            assert data_set.get_row_size() > 0
            result2.extend(data_set)
        assert next_count > 1
        # assert result1 == result2

    def test_scan_tag_with_no_existed_tag_name(self):
        try:
            resp = self.graph_storage_client.scan_vertex(
                space_name="test_graph_storage_client", tag_name="not_existed", limit=1
            )
            assert False
        except TagNotFoundException:
            assert True
        except Exception as e:
            assert False, e

    def test_scan_edge_data(self):
        resp = self.graph_storage_client.scan_edge(
            space_name="test_graph_storage_client", edge_name="friend", limit=1
        )
        assert resp.has_next()
        result = resp.next()
        data_set = result.get_data_set_wrapper()
        assert data_set.get_row_size() == 10
        assert data_set.get_col_names() == [
            "friend._src",
            "friend._type",
            "friend._rank",
            "friend._dst",
            "friend.start",
            "friend.end",
        ]
        # test as edge
        assert len(result.as_relationships()) >= 10

        # test iterator
        count = 0
        for edge in result:
            count += 1
            assert isinstance(edge, EdgeData)
            relationship = edge.as_relationship()
            assert relationship.keys() == ["start", "end"]
            assert relationship.edge_name() == "friend"
            assert relationship.start_vertex_id().as_string().find("person") >= 0
            assert relationship.end_vertex_id().as_string().find("person") >= 0
            # test get_prop_values
            prop_values = edge.get_prop_values()
            assert len(prop_values) == 2
            assert prop_values[0].is_int()
            assert prop_values[0].is_int() < 2010
            assert prop_values[1].is_int()
            assert prop_values[1].as_int() >= 2010
        assert count > 1

    @pytest.mark.skip(reason="cannot test with next cursor yet")
    def test_scan_edge(self):
        # test get all by once
        resp = self.graph_storage_client.scan_edge(
            space_name="test_graph_storage_client", edge_name="friend", limit=2000
        )
        next_count = 0
        while resp.has_next():
            next_count += 1
            result = resp.next()
            data_set = result.get_data_set_wrapper()
            assert data_set.get_row_size() == 1000
        assert next_count == 1

        # test with cursor
        resp = self.graph_storage_client.scan_edge(
            space_name="test_graph_storage_client", edge_name="friend", limit=10
        )
        next_count = 0
        while resp.has_next():
            next_count += 1
            result = resp.next()
            data_set = result.get_data_set_wrapper()
            assert data_set.get_row_size() > 0
        assert next_count > 1

    def test_scan_edge_with_no_existed_edge_name(self):
        try:
            self.graph_storage_client.scan_edge(
                space_name="test_graph_storage_client", edge_name="not_existed", limit=1
            )
            assert False
        except EdgeNotFoundException:
            assert True
        except Exception as e:
            assert False, e

    def test_scan_edge_with_leader(self):
        try:
            resp = self.graph_storage_client.scan_edge(
                space_name="test_graph_storage_client",
                edge_name="friend",
                limit=2000,
                enable_read_from_follower=False,
            )
        except Exception as e:
            assert False, e

    @pytest.mark.skip(reason="nebula-storage is not return the leader address")
    def test_scan_edge_with_leader_change(self):
        os.system("docker stop nebula-docker-compose_storaged0_1")
        try:
            resp = self.graph_storage_client.scan_edge(
                space_name="test_graph_storage_client",
                edge_name="friend",
                limit=2000,
                enable_read_from_follower=False,
            )
            next_count = 0
            while resp.has_next():
                next_count += 1
                result = resp.next()
                data_set = result.get_data_set_wrapper()
                assert data_set.get_row_size() == 1000
            assert next_count == 1
        except Exception as e:
            assert False, e
        finally:
            os.system("docker start nebula-docker-compose_storaged0_1")
