#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import json
import time
from unittest import TestCase

from nebula3.common.ttypes import Date, DateTime, ErrorCode, Time
from nebula3.Config import Config
from nebula3.data.DataObject import DateTimeWrapper, DateWrapper, Null, TimeWrapper
from nebula3.gclient.net import ConnectionPool


class TestBaseCase(TestCase):
    pool = None
    session = None

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        configs = Config()
        configs.max_connection_pool_size = 1
        cls.pool = ConnectionPool()
        cls.pool.init([("127.0.0.1", 9671)], configs)
        cls.session = cls.pool.get_session("root", "nebula")
        resp = cls.session.execute(
            """
            CREATE SPACE IF NOT EXISTS test_data(vid_type=FIXED_STRING(8));
            USE test_data;
            CREATE TAG IF NOT EXISTS person(name string, age int8, grade int16,
            friends int32, book_num int64, birthday datetime,
            start_school date, morning time, property double,
            is_girl bool, child_name fixed_string(10), expend float,
            first_out_city timestamp, hobby string);
            CREATE TAG IF NOT EXISTS student(name string, interval duration);
            CREATE EDGE IF NOT EXISTS like(likeness double);
            CREATE EDGE IF NOT EXISTS friend(start_year int, end_year int);
            CREATE TAG INDEX IF NOT EXISTS person_name_index ON person(name(8));
            """
        )
        assert resp.is_succeeded(), resp.error_msg()

        time.sleep(5)
        resp = cls.session.execute(
            "INSERT VERTEX person(name, age, grade,friends, book_num,"
            "birthday, start_school, morning, property,"
            "is_girl, child_name, expend, first_out_city) VALUES"
            "'Bob':('Bob', 10, 3, 10, 100, datetime('2010-09-10T10:08:02'),"
            "date('2017-09-10'), time('07:10:00'), "
            "1000.0, false, 'Hello World!', 100.0, 1111),"
            "'Lily':('Lily', 9, 3, 10, 100, datetime('2010-09-10T10:08:02'), "
            "date('2017-09-10'), time('07:10:00'), "
            "1000.0, false, 'Hello World!', 100.0, 1111),"
            "'Tom':('Tom', 10, 3, 10, 100, datetime('2010-09-10T10:08:02'), "
            "date('2017-09-10'), time('07:10:00'), "
            "1000.0, false, 'Hello World!', 100.0, 1111),"
            "'Jerry':('Jerry', 9, 3, 10, 100, datetime('2010-09-10T10:08:02'),"
            "date('2017-09-10'), time('07:10:00'), "
            "1000.0, false, 'Hello World!', 100.0, 1111), "
            "'John':('John', 10, 3, 10, 100, datetime('2010-09-10T10:08:02'), "
            "date('2017-09-10'), time('07:10:00'), "
            "1000.0, false, 'Hello World!', 100.0, 1111)"
        )
        assert resp.is_succeeded(), resp.error_msg()
        resp = cls.session.execute(
            "INSERT VERTEX student(name, interval) VALUES "
            "'Bob':('Bob', duration({months:1, seconds:100, microseconds:20})),"
            "'Lily':('Lily', duration({years: 1, seconds: 0})),"
            "'Tom':('Tom', duration({years: 1, seconds: 0})),"
            "'Jerry':('Jerry', duration({years: 1, seconds: 0})),"
            "'John':('John', duration({years: 1, seconds: 0}))"
        )
        assert resp.is_succeeded(), resp.error_msg()

        resp = cls.session.execute(
            "INSERT EDGE like(likeness) VALUES "
            "'Bob'->'Lily':(80.0), "
            "'Bob'->'Tom':(70.0), "
            "'Jerry'->'Lily':(84.0),"
            "'Tom'->'Jerry':(68.3), "
            "'Bob'->'John':(97.2)"
        )
        assert resp.is_succeeded(), resp.error_msg()
        resp = cls.session.execute(
            "INSERT EDGE friend(start_year, end_year) VALUES "
            "'Bob'->'Lily':(2018, 2020), "
            "'Bob'->'Tom':(2018, 2020), "
            "'Jerry'->'Lily':(2018, 2020),"
            "'Tom'->'Jerry':(2018, 2020), "
            "'Bob'->'John':(2018, 2020)"
        )
        assert resp.is_succeeded(), resp.error_msg()

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.session is None:
            cls.session.release()
        if cls.pool is None:
            cls.pool.close()

    def test_base_type(self):
        resp = self.session.execute(
            'FETCH PROP ON person "Bob" YIELD person.name, person.age, person.grade,'
            "person.friends, person.book_num, person.birthday, "
            "person.start_school, person.morning, "
            "person.property, person.is_girl, person.child_name, "
            "person.expend, person.first_out_city, person.hobby"
        )
        assert resp.is_succeeded(), resp.error_msg()
        assert "" == resp.error_msg()
        assert resp.latency() > 0
        assert "" == resp.comment()
        assert ErrorCode.SUCCEEDED == resp.error_code()
        assert "test_data" == resp.space_name()
        assert not resp.is_empty()
        assert 1 == resp.row_size()
        names = [
            "person.name",
            "person.age",
            "person.grade",
            "person.friends",
            "person.book_num",
            "person.birthday",
            "person.start_school",
            "person.morning",
            "person.property",
            "person.is_girl",
            "person.child_name",
            "person.expend",
            "person.first_out_city",
            "person.hobby",
        ]
        assert names == resp.keys()

        assert "Bob" == resp.row_values(0)[0].as_string()
        assert 10 == resp.row_values(0)[1].as_int()
        assert 3 == resp.row_values(0)[2].as_int()
        assert 10 == resp.row_values(0)[3].as_int()
        assert 100 == resp.row_values(0)[4].as_int()
        return_data_time_val = resp.row_values(0)[5].as_datetime()
        assert return_data_time_val == DateTimeWrapper(
            DateTime(2010, 9, 10, 2, 8, 2, 0)
        )
        assert (
            "2010-09-10T10:08:02.000000"
            == return_data_time_val.get_local_datetime_str()
        )
        assert (
            "utc datetime: 2010-09-10T02:08:02.000000, timezone_offset: 28800"
            == str(return_data_time_val)
        )

        assert DateWrapper(Date(2017, 9, 10)) == resp.row_values(0)[6].as_date()

        expected_time_val = TimeWrapper(Time(23, 10, 0, 0))
        return_time_val = resp.row_values(0)[7].as_time()
        assert expected_time_val == return_time_val
        assert "07:10:00.000000" == return_time_val.get_local_time_str()
        assert "utc time: 23:10:00.000000, timezone_offset: 28800" == str(
            return_time_val
        )

        assert 1000.0 == resp.row_values(0)[8].as_double()
        assert False == resp.row_values(0)[9].as_bool()
        assert "Hello Worl" == resp.row_values(0)[10].as_string()
        assert 100.0 == resp.row_values(0)[11].as_double()
        assert 1111 == resp.row_values(0)[12].as_int()
        assert Null(Null.__NULL__) == resp.row_values(0)[13].as_null()

    def test_list_type(self):
        resp = self.session.execute("YIELD ['name', 'age', 'birthday'];")
        assert resp.is_succeeded()
        assert 1 == resp.row_size()
        result = [name.as_string() for name in resp.row_values(0)[0].as_list()]
        assert ["name", "age", "birthday"] == result

    def test_set_type(self):
        resp = self.session.execute("YIELD {'name', 'name', 'age', 'birthday'};")
        assert resp.is_succeeded()
        assert 1 == resp.row_size()
        assert resp.row_values(0)[0].is_set()
        result = [name.as_string() for name in resp.row_values(0)[0].as_set()]
        assert sorted(["name", "age", "birthday"]) == sorted(result)

    def test_map_type(self):
        resp = self.session.execute(
            "YIELD {name:'Tom', age:18, birthday: '2010-10-10'};"
        )
        assert resp.is_succeeded()
        assert 1 == resp.row_size()
        assert resp.row_values(0)[0].is_map()
        val = resp.row_values(0)[0].as_map()
        assert len(val.keys()) == 3
        assert "name" in val.keys()
        assert val["name"].as_string() == "Tom"
        assert "age" in val.keys()
        assert val["age"].as_int() == 18
        assert "birthday" in val.keys()
        assert val["birthday"].as_string() == "2010-10-10"

    def test_node_type(self):
        resp = self.session.execute('MATCH (v:person {name: "Bob"}) RETURN v')
        assert resp.is_succeeded()
        assert 1 == resp.row_size()
        assert resp.row_values(0)[0].as_node()

    def test_relationship_type(self):
        resp = self.session.execute(
            'MATCH (:person{name: "Bob"}) -[e:friend]-> (:person{name: "Lily"}) RETURN e'
        )
        assert resp.is_succeeded()
        assert 1 == resp.row_size()
        assert resp.row_values(0)[0].is_edge()
        rel = resp.row_values(0)[0].as_relationship()

        assert '("Bob")-[:friend@0{end_year: 2020, start_year: 2018}]->("Lily")' == str(
            rel
        )

    def test_path_type(self):
        resp = self.session.execute(
            'MATCH p = (:person{name: "Bob"})-[:friend]->(:person{name: "Lily"}) return p'
        )
        assert resp.is_succeeded()
        assert 1 == resp.row_size()
        assert resp.row_values(0)[0].is_path()
        path = resp.row_values(0)[0].as_path()
        expected_str = (
            '("Bob" :student{interval: P1MT100.000020000S, name: "Bob"} :person{age:'
            " 10, birthday: utc datetime: 2010-09-10T02:08:02.000000, timezone_offset:"
            ' 28800, book_num: 100, child_name: "Hello Worl", expend: 100.0,'
            " first_out_city: 1111, friends: 10, grade: 3, hobby: __NULL__, is_girl:"
            " False, morning: utc time: 23:10:00.000000, timezone_offset: 28800, name:"
            ' "Bob", property: 1000.0, start_school:'
            ' 2017-09-10})-[:friend@0{start_year: 2018, end_year: 2020}]->("Lily"'
            ' :student{interval: P12MT0.000000000S, name: "Lily"} :person{age: 9,'
            " birthday: utc datetime: 2010-09-10T02:08:02.000000, timezone_offset:"
            ' 28800, book_num: 100, child_name: "Hello Worl", expend: 100.0,'
            " first_out_city: 1111, friends: 10, grade: 3, hobby: __NULL__, is_girl:"
            " False, morning: utc time: 23:10:00.000000, timezone_offset: 28800, name:"
            ' "Lily", property: 1000.0, start_school: 2017-09-10})'
        )
        assert expected_str == str(path)

        assert resp.whole_latency() > 100


class TestExecuteJson(TestBaseCase):
    def test_basic_types(self):
        resp = self.session.execute_json(
            'YIELD 1, 2.2, "hello", [1,2,"abc"], {key: "value"}, "汉字"'
        )
        exp = [1, 2.2, "hello", [1, 2, "abc"], {"key": "value"}, "汉字"]
        json_obj = json.loads(resp)

        # Get errorcode
        resp_error_code = json_obj["errors"][0]["code"]
        assert 0 == resp_error_code

        # Get data
        assert exp == json_obj["results"][0]["data"][0]["row"]

        # Get space name
        respSpace = json_obj["results"][0]["spaceName"]
        assert "test_data" == respSpace

    def test_complex_types(self):
        resp = self.session.execute_json('MATCH (v:person {name: "Bob"}) RETURN v')
        exp = [
            {
                "person.age": 10,
                "person.birthday": "2010-09-10T02:08:02.000000000Z",
                "person.book_num": 100,
                "person.child_name": "Hello Worl",
                "person.expend": 100,
                "person.first_out_city": 1111,
                "person.friends": 10,
                "person.grade": 3,
                "person.hobby": None,
                "person.is_girl": False,
                "person.morning": "23:10:00.000000000Z",
                "person.name": "Bob",
                "person.property": 1000,
                "person.start_school": "2017-09-10",
                "student.name": "Bob",
                "student.interval": "P1MT100.000020000S",
            }
        ]
        json_obj = json.loads(resp)
        assert exp == json_obj["results"][0]["data"][0]["row"]

    def test_error(self):
        resp = self.session.execute_json('MATCH (v:invalidTag {name: "Bob"}) RETURN v')

        json_obj = json.loads(resp)

        error_code = -1009
        resp_error_code = json_obj["errors"][0]["code"]
        assert error_code == resp_error_code

        error_msg = "SemanticError: `invalidTag': Unknown tag"
        resp_error_msg = json_obj["errors"][0]["message"]
        assert error_msg == resp_error_msg
