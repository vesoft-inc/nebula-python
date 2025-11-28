#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import time
import json

from nebula3.gclient.net import ConnectionPool, ExecuteError
from nebula3.Config import Config
from nebula3.common import *
from unittest import TestCase


class TestParameter(TestCase):
    @classmethod
    def setUp(self) -> None:
        super().setUpClass()
        self.user_name = "root"
        self.password = "nebula"
        self.configs = Config()
        self.configs.max_connection_pool_size = 6
        self.pool = ConnectionPool()
        self.pool.init([("127.0.0.1", 9671)], self.configs)

        # get session from the pool
        client = self.pool.get_session("root", "nebula")
        assert client is not None

        # prepare space and insert data
        resp = client.execute(
            "CREATE SPACE IF NOT EXISTS parameter_test(vid_type=FIXED_STRING(30));USE parameter_test"
        )
        assert resp.is_succeeded(), resp.error_msg()
        resp = client.execute(
            "CREATE TAG IF NOT EXISTS person(name string, age int);"
            "CREATE EDGE like (likeness double);"
        )

        time.sleep(6)
        # insert data need to sleep after create schema
        resp = client.execute("CREATE TAG INDEX person_age_index on person(age)")
        time.sleep(6)
        # insert vertex
        resp = client.execute(
            'INSERT VERTEX person(name, age) VALUES "Bob":("Bob", 10), "Lily":("Lily", 9)'
        )
        assert resp.is_succeeded(), resp.error_msg()
        # insert edges
        resp = client.execute('INSERT EDGE like(likeness) VALUES "Bob"->"Lily":(80.0);')
        assert resp.is_succeeded(), resp.error_msg()
        resp = client.execute("REBUILD TAG INDEX person_age_index")
        assert resp.is_succeeded(), resp.error_msg()

        # prepare parameters
        bval = ttypes.Value()
        bval.set_bVal(True)
        ival = ttypes.Value()
        ival.set_iVal(3)
        sval = ttypes.Value()
        sval.set_sVal("Bob")
        self.params = {"p1": ival, "p2": bval, "p3": sval}
        self.params_premitive = {
            "p1": 3,
            "p2": True,
            "p3": "Bob",
            "p4": ["Bob", "Lily"],
        }

        assert self.pool.connects() == 1
        assert self.pool.in_used_connects() == 1

    def test_parameter(self):
        # get session from the pool
        client = self.pool.get_session("root", "nebula")
        assert client is not None
        resp = client.execute_parameter(
            "USE parameter_test",
            self.params,
        )
        assert resp.is_succeeded()
        # test basic parameter
        resp = client.execute_parameter(
            "RETURN abs($p1)+3 AS col1, (toBoolean($p2) and false) AS col2, toLower($p3)+1 AS col3",
            self.params,
        )
        assert resp.is_succeeded(), resp.error_msg()
        assert 1 == resp.row_size()
        names = ["col1", "col2", "col3"]
        assert names == resp.keys()
        assert 6 == resp.row_values(0)[0].as_int()
        assert False == resp.row_values(0)[1].as_bool()
        assert "bob1" == resp.row_values(0)[2].as_string()

        # test cypher parameter
        resp = client.execute_parameter(
            f"""MATCH (v:person)--() WHERE v.person.age>abs($p1)+3
            RETURN v.person.name AS vname,v.person.age AS vage ORDER BY vage, $p3 LIMIT $p1+1""",
            self.params,
        )
        assert resp.is_succeeded(), resp.error_msg()
        assert 2 == resp.row_size()
        names = ["vname", "vage"]
        assert names == resp.keys()
        assert "Lily" == resp.row_values(0)[0].as_string()
        assert 9 == resp.row_values(0)[1].as_int()
        assert "Bob" == resp.row_values(1)[0].as_string()
        assert 10 == resp.row_values(1)[1].as_int()
        # test ngql parameter
        resp = client.execute_parameter(
            '$p1=go from "Bob" over like yield like._dst;',
            self.params,
        )
        assert not resp.is_succeeded()
        resp = client.execute_parameter(
            "go from $p3 over like yield like._dst;",
            self.params,
        )
        assert not resp.is_succeeded()
        resp = client.execute_parameter(
            "fetch prop on person $p3 yield vertex as v",
            self.params,
        )
        assert not resp.is_succeeded()
        resp = client.execute_parameter(
            'find all path from $p3 to "Yao Ming" over like yield path as p',
            self.params,
        )
        assert not resp.is_succeeded()
        resp = client.execute_parameter(
            "get subgraph from $p3 both like yield vertices as v",
            self.params,
        )
        assert not resp.is_succeeded()
        resp = client.execute_parameter(
            'go 3 steps from "Bob" over like yield like._dst limit [1,$p1,3]',
            self.params,
        )
        assert not resp.is_succeeded()

        # same test with premitive params
        resp = client.execute_py(
            "RETURN abs($p1)+3 AS col1, (toBoolean($p2) and false) AS col2, toLower($p3)+1 AS col3",
            self.params_premitive,
        ).as_primitive()
        assert 1 == len(resp)
        assert ["col1", "col2", "col3"] == list(resp[0].keys())
        assert resp[0]["col1"] == 6
        assert resp[0]["col2"] == False
        assert resp[0]["col3"] == "bob1"
        try:
            resp = client.execute_py(
                '$p1=go from "Bob" over like yield like._dst;',
                self.params_premitive,
            )
        except ExecuteError:
            pass
        else:
            raise AssertionError("should raise exception")
        try:
            resp = client.execute_py(
                "go from $p3 over like yield like._dst;",
                self.params_premitive,
            )
        except ExecuteError:
            pass
        else:
            raise AssertionError("should raise exception")
        resp = client.execute_py(
            "MATCH (v) WHERE id(v) in $p4 RETURN id(v) AS vertex_id",
            self.params_premitive,
        ).as_primitive()
        assert 2 == len(resp)

    def tearDown(self) -> None:
        client = self.pool.get_session("root", "nebula")
        assert client is not None
        resp = client.execute("DROP SPACE parameter_test")
        assert resp.is_succeeded(), resp.error_msg()
