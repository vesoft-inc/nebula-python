#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import copy
from datetime import date
from unittest import TestCase

from nebula3.common import ttypes
from nebula3.common.ttypes import (
    Date,
    DateTime,
    Duration,
    Edge,
    ErrorCode,
    NList,
    NMap,
    NSet,
    NullType,
    Time,
    Value,
    Vertex,
)

from nebula3.data.DataObject import (
    DataSetWrapper,
    DateTimeWrapper,
    DateWrapper,
    DurationWrapper,
    GeographyWrapper,
    Node,
    Null,
    PathWrapper,
    Relationship,
    TimeWrapper,
    ValueWrapper,
)
from nebula3.data.ResultSet import ResultSet
from nebula3.Exception import InvalidKeyException
from nebula3.graph import ttypes as graphTtype


class TestBaseCase(TestCase):
    @classmethod
    def get_vertex_value(cls, vid, empty_props=False):
        vertex = ttypes.Vertex()
        vertex.vid = ttypes.Value(sVal=vid)
        vertex.tags = list()
        for i in range(0, 3):
            tag = ttypes.Tag()
            tag.name = ("tag{}".format(i)).encode("utf-8")
            if not empty_props:
                tag.props = dict()
                for j in range(0, 5):
                    value = ttypes.Value()
                    value.set_iVal(j)
                    tag.props[("prop{}".format(j)).encode("utf-8")] = value
            vertex.tags.append(tag)
        return vertex

    @classmethod
    def get_edge_value(cls, src_id, dst_id, is_reverse=False, empty_props=False):
        edge = ttypes.Edge()
        if not is_reverse:
            edge.src = ttypes.Value(sVal=src_id)
            edge.dst = ttypes.Value(sVal=dst_id)
        else:
            edge.src = ttypes.Value(sVal=dst_id)
            edge.dst = ttypes.Value(sVal=src_id)
        edge.type = 1
        edge.name = b"classmate"
        edge.ranking = 100
        if not empty_props:
            edge.props = dict()
            for i in range(0, 5):
                value = ttypes.Value()
                value.set_iVal(i)
                edge.props[("prop{}".format(i)).encode("utf-8")] = value
        return edge

    @classmethod
    def get_path_value(cls, start_id, steps=5):
        path = ttypes.Path()
        path.src = cls.get_vertex_value(start_id)
        path.steps = list()
        for i in range(0, steps):
            step = ttypes.Step()
            step.dst = cls.get_vertex_value(("vertex{}".format(i)).encode("utf-8"))
            step.type = 1 if i % 2 == 0 else -1
            step.name = b"classmate"
            step.ranking = 100
            step.props = dict()
            for i in range(0, 5):
                value = ttypes.Value()
                value.set_iVal(i)
                step.props[("prop{}".format(i)).encode("utf-8")] = value
            path.steps.append(step)
        return path

    @classmethod
    def get_geography_value(cls, x, y):
        coord = ttypes.Coordinate()
        coord.x = x
        coord.y = y
        point = ttypes.Point()
        point.coord = coord
        geog = ttypes.Geography()
        geog.set_ptVal(point)
        return geog

    @classmethod
    def get_data_set(cls):
        data_set = ttypes.DataSet()
        data_set.column_names = [
            b"col1_empty",
            b"col2_null",
            b"col3_bool",
            b"col4_int",
            b"col5_double",
            b"col6_string",
            b"col7_list",
            b"col8_set",
            b"col9_map",
            b"col10_time",
            b"col11_date",
            b"col12_datetime",
            b"col13_vertex",
            b"col14_edge",
            b"col15_path",
            b"col16_geography",
            b"col17_duration",
        ]
        row = ttypes.Row()
        row.values = []
        value1 = ttypes.Value()
        row.values.append(value1)
        value2 = ttypes.Value()
        value2.set_nVal(NullType.BAD_DATA)
        row.values.append(value2)
        value3 = ttypes.Value()
        value3.set_bVal(False)
        row.values.append(value3)
        value4 = ttypes.Value()
        value4.set_iVal(100)
        row.values.append(value4)
        value5 = ttypes.Value()
        value5.set_fVal(10.01)
        row.values.append(value5)
        value6 = ttypes.Value()
        value6.set_sVal(b"hello world")
        row.values.append(value6)
        value7 = ttypes.Value()
        str_val1 = ttypes.Value()
        str_val1.set_sVal(b"word")
        str_val2 = ttypes.Value()
        str_val2.set_sVal(b"car")
        list_val = NList()
        list_val.values = [str_val1, str_val2]
        value7.set_lVal(list_val)
        row.values.append(value7)
        value8 = ttypes.Value()
        set_val = NSet()
        set_val.values = set()
        set_val.values.add(str_val1)
        set_val.values.add(str_val2)
        value8.set_uVal(set_val)
        row.values.append(value8)
        value9 = ttypes.Value()
        map = NMap()
        map.kvs = {b"a": str_val1, b"b": str_val2}
        value9.set_mVal(map)
        row.values.append(value9)
        value10 = ttypes.Value()
        value10.set_tVal(Time(10, 10, 10, 10000))
        row.values.append(value10)
        value11 = ttypes.Value()
        value11.set_dVal(date(2020, 10, 1))
        row.values.append(value11)
        value12 = ttypes.Value()
        value12.set_dtVal(DateTime(2020, 10, 1, 10, 10, 10, 10000))
        row.values.append(value12)
        value13 = ttypes.Value()
        value13.set_vVal(cls.get_vertex_value(b"Tom"))
        row.values.append(value13)
        value14 = ttypes.Value()
        value14.set_eVal(cls.get_edge_value(b"Tom", b"Lily"))
        row.values.append(value14)
        value15 = ttypes.Value()
        value15.set_pVal(cls.get_path_value(b"Tom", 3))
        row.values.append(value15)
        value16 = ttypes.Value()
        value16.set_ggVal(cls.get_geography_value(4.8, 5.2))
        row.values.append(value16)
        value17 = ttypes.Value()
        value17.set_duVal(Duration(86400, 3000, 12))
        row.values.append(value17)
        data_set.rows = []
        data_set.rows.append(row)
        data_set.rows.append(row)
        return data_set

    @classmethod
    def get_result_set(cls):
        resp = graphTtype.ExecutionResponse()
        resp.error_code = ErrorCode.E_BAD_PERMISSION
        resp.error_msg = b"Permission"
        resp.comment = b"Permission"
        resp.space_name = b"test"
        resp.latency_in_us = 100

        resp.data = cls.get_data_set()
        return ResultSet(resp, 100)


class TesValueWrapper(TestBaseCase):
    def test_as_bool(self):
        value = ttypes.Value()
        value.set_bVal(False)
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_bool()

        node = value_wrapper.as_bool()
        assert isinstance(node, bool)

    def test_as_int(self):
        value = ttypes.Value()
        value.set_iVal(100)
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_int()

        node = value_wrapper.as_int()
        assert isinstance(node, int)

    def test_as_double(self):
        value = ttypes.Value()
        value.set_fVal(10.10)
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_double()

        node = value_wrapper.as_double()
        assert isinstance(node, float)

    def test_as_string(self):
        value = ttypes.Value()
        value.set_sVal(b"Tom")
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_string()

        str_val = value_wrapper.as_string()
        assert isinstance(str_val, str)

    def test_as_list(self):
        value = ttypes.Value()
        str_val1 = ttypes.Value()
        str_val1.set_sVal(b"word")
        str_val2 = ttypes.Value()
        str_val2.set_sVal(b"car")
        val_list = NList()
        val_list.values = [str_val1, str_val2]
        value.set_lVal(val_list)
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_list()

        list_val = value_wrapper.as_list()
        assert isinstance(list_val, list)
        expect_result = [
            ValueWrapper(ttypes.Value(sVal=b"word")),
            ValueWrapper(ttypes.Value(sVal=b"car")),
        ]
        assert list_val == expect_result

    def test_as_set(self):
        value = ttypes.Value()
        str_val1 = ttypes.Value()
        str_val1.set_sVal(b"word")
        str_val2 = ttypes.Value()
        str_val2.set_sVal(b"car")
        set_val = NSet()
        set_val.values = set()
        set_val.values.add(str_val1)
        set_val.values.add(str_val2)
        value.set_uVal(set_val)
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_set()

        set_val = value_wrapper.as_set()
        assert isinstance(set_val, set)
        expect_result = set()
        expect_result.add(ValueWrapper(ttypes.Value(sVal=b"word")))
        expect_result.add(ValueWrapper(ttypes.Value(sVal=b"car")))
        assert set_val == expect_result

    def test_as_map(self):
        value = ttypes.Value()
        str_val1 = ttypes.Value()
        str_val1.set_sVal(b"word")
        str_val2 = ttypes.Value()
        str_val2.set_sVal(b"car")
        map_val = NMap()
        map_val.kvs = {b"a": str_val1, b"b": str_val2}
        value.set_mVal(map_val)
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_map()

        map_val = value_wrapper.as_map()
        assert isinstance(map_val, dict)
        expect_result = dict()
        expect_result["a"] = ValueWrapper(ttypes.Value(sVal=b"word"))
        expect_result["b"] = ValueWrapper(ttypes.Value(sVal=b"car"))
        assert map_val == expect_result

    def test_cast(self):
        value = ttypes.Value()

        bool_val = ttypes.Value()
        bool_val.set_bVal(False)

        int_val = ttypes.Value()
        int_val.set_iVal(100)

        float_val = ttypes.Value()
        float_val.set_fVal(10.10)

        str_val1 = ttypes.Value()
        str_val1.set_sVal(b"word")

        str_val2 = ttypes.Value()
        str_val2.set_sVal(b"car")

        set_val = ttypes.Value()
        tmp_set_val = NSet()
        tmp_set_val.values = set()
        tmp_set_val.values.add(str_val1)
        tmp_set_val.values.add(str_val2)
        set_val.set_uVal(tmp_set_val)

        map_val = ttypes.Value()
        tmp_map_val = NMap()
        tmp_map_val.kvs = {b"a": str_val1, b"b": str_val2}
        map_val.set_mVal(tmp_map_val)

        node_val = ttypes.Value()
        node_val.set_vVal(self.get_vertex_value(b"Tom"))

        relationship_val = ttypes.Value(eVal=self.get_edge_value(b"Tom", b"Lily"))

        path_val = ttypes.Value()
        path_val.set_pVal(self.get_path_value(b"Tom"))

        tmp_list_val = NList()
        tmp_list_val.values = [
            bool_val,
            int_val,
            float_val,
            str_val1,
            str_val2,
            set_val,
            map_val,
            node_val,
            relationship_val,
            path_val,
        ]
        value.set_lVal(tmp_list_val)

        value = ValueWrapper(value)

        list_val = value.cast()
        assert isinstance(list_val, list)

        expect_result = [
            False,
            100,
            10.10,
            "word",
            "car",
            {"word", "car"},
            {"a": "word", "b": "car"},
            ValueWrapper(node_val).as_node(),
            ValueWrapper(relationship_val).as_relationship(),
            ValueWrapper(path_val).as_path(),
        ]
        assert list_val == expect_result

    def test_cast_primitive(self):
        # Test casting for primitive types

        # Test boolean
        bool_val = ttypes.Value(bVal=True)
        assert ValueWrapper(bool_val).cast_primitive() is True

        # Test integer
        int_val = ttypes.Value(iVal=42)
        assert ValueWrapper(int_val).cast_primitive() == 42

        # Test double
        double_val = ttypes.Value(fVal=3.14)
        assert ValueWrapper(double_val).cast_primitive() == 3.14

        # Test string
        string_val = ttypes.Value(sVal=b"hello")
        assert ValueWrapper(string_val).cast_primitive() == "hello"

        # Test null
        null_val = ttypes.Value(nVal=ttypes.NullType.__NULL__)
        assert ValueWrapper(null_val).cast_primitive() is None

        # Test geography
        geography_val = ttypes.Value(ggVal=self.get_geography_value(3.0, 5.2))
        geography_raw = ValueWrapper(geography_val)
        geography = geography_raw.as_geography()
        assert geography_raw.cast_primitive() == geography.__repr__()

        # Test duration
        duration_val = ttypes.Value(duVal=Duration(86400, 3000, 12))
        duration_raw = ValueWrapper(duration_val)
        duration = duration_raw.as_duration()
        assert duration_raw.cast_primitive() == duration.__repr__()

        # Test list
        list_val = ttypes.Value()
        str_val1 = ttypes.Value()
        str_val1.set_sVal(b"word")
        str_val2 = ttypes.Value()
        str_val2.set_sVal(b"car")
        val_list = NList()
        val_list.values = [str_val1, str_val2]
        list_val.set_lVal(val_list)
        list_raw = ValueWrapper(list_val)
        assert list_raw.cast_primitive() == [
            ValueWrapper(str_val1).cast_primitive(),
            ValueWrapper(str_val2).cast_primitive(),
        ]

        # Test set
        set_val = ttypes.Value()
        tmp_set_val = NSet()
        tmp_set_val.values = set()
        tmp_set_val.values.add(str_val1)
        tmp_set_val.values.add(str_val2)
        set_val.set_uVal(tmp_set_val)
        set_raw = ValueWrapper(set_val)
        assert set_raw.cast_primitive() == {
            ValueWrapper(str_val1).cast_primitive(),
            ValueWrapper(str_val2).cast_primitive(),
        }

        # Test map
        map_val = ttypes.Value()
        tmp_map_val = NMap()
        tmp_map_val.kvs = {b"a": str_val1, b"b": str_val2}
        map_val.set_mVal(tmp_map_val)
        map_raw = ValueWrapper(map_val)
        assert map_raw.cast_primitive() == {
            "a": ValueWrapper(str_val1).cast_primitive(),
            "b": ValueWrapper(str_val2).cast_primitive(),
        }

        # Test time
        time_val = ttypes.Value()
        time_val.set_tVal(Time(10, 10, 10, 10000))
        time_raw = ValueWrapper(time_val)
        time = time_raw.as_time()
        assert time_raw.cast_primitive() == time.get_local_time_str()

    def test_as_time(self):
        time = Time()
        time.hour = 10
        time.minute = 20
        time.sec = 10
        time.microsec = 100
        value = ttypes.Value(tVal=time)
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_time()

        time_val = value_wrapper.as_time()
        time_val.set_timezone_offset(28800)
        assert isinstance(time_val, TimeWrapper)
        assert time_val.get_hour() == 10
        assert time_val.get_minute() == 20
        assert time_val.get_sec() == 10
        assert time_val.get_microsec() == 100
        assert "utc time: 10:20:10.000100, timezone_offset: 28800" == str(time_val)
        assert "18:20:10.000100" == time_val.get_local_time_str()
        new_time = copy.deepcopy(time)
        new_time.hour = 18
        assert new_time == time_val.get_local_time()

        new_time_2 = copy.deepcopy(time)
        new_time_2.hour = 12
        assert new_time_2 == time_val.get_local_time_by_timezone_offset(7200)

    def test_as_date(self):
        date = Date()
        date.year = 220
        date.month = 2
        date.day = 10
        value = ttypes.Value(dVal=date)
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_date()

        date_val = value_wrapper.as_date()
        assert isinstance(date_val, DateWrapper)
        assert date_val.get_year() == 220
        assert date_val.get_month() == 2
        assert date_val.get_day() == 10
        assert "220-02-10" == str(date_val)

    def test_as_datetime(self):
        datetime = DateTime()
        datetime.year = 123
        datetime.month = 2
        datetime.day = 1
        datetime.hour = 10
        datetime.minute = 20
        datetime.sec = 10
        datetime.microsec = 100
        value = ttypes.Value(dtVal=datetime)
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_datetime()

        datetime_val = value_wrapper.as_datetime()
        datetime_val.set_timezone_offset(28800)
        assert isinstance(datetime_val, DateTimeWrapper)
        assert datetime_val.get_hour() == 10
        assert datetime_val.get_minute() == 20
        assert datetime_val.get_sec() == 10
        assert datetime_val.get_microsec() == 100
        assert "utc datetime: 123-02-01T10:20:10.000100, timezone_offset: 28800" == str(
            datetime_val
        )
        assert "123-02-01T18:20:10.000100" == datetime_val.get_local_datetime_str()
        new_datetime = copy.deepcopy(datetime)
        new_datetime.hour = 18
        assert new_datetime == datetime_val.get_local_datetime()

        new_datetime_2 = copy.deepcopy(datetime)
        new_datetime_2.hour = 12
        assert new_datetime_2 == datetime_val.get_local_datetime_by_timezone_offset(
            7200
        )

    def test_as_node(self):
        value = ttypes.Value()
        value.set_vVal(self.get_vertex_value(b"Tom"))
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_vertex()

        node = value_wrapper.as_node()
        assert isinstance(node, Node)
        assert node.get_id().as_string() == "Tom"
        assert node.has_tag("tag1")
        assert (
            node.prop_names("tag1").sort()
            == ["prop0", "prop1", "prop2", "prop3", "prop4"].sort()
        )
        expect_values = [(v.as_int()) for v in node.prop_values("tag1")]
        assert expect_values == [0, 1, 2, 3, 4]
        assert node.tags() == ["tag0", "tag1", "tag2"]
        assert (
            list(node.properties("tag1").keys()).sort()
            == ["prop0", "prop1", "prop2", "prop3", "prop4"].sort()
        )
        expect_values = [(v.as_int()) for v in node.properties("tag1").values()]
        assert expect_values == [0, 1, 2, 3, 4]

    def test_as_relationship(self):
        value = ttypes.Value(eVal=self.get_edge_value(b"Tom", b"Lily"))
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_edge()

        relationship = value_wrapper.as_relationship()
        assert isinstance(relationship, Relationship)

        # test with reversely
        reversely_value = ttypes.Value(eVal=self.get_edge_value(b"Lily", b"Tom", True))
        reversely_value_wrapper = ValueWrapper(reversely_value)
        reversely_relationship = reversely_value_wrapper.as_relationship()
        assert isinstance(reversely_relationship, Relationship)
        assert reversely_relationship == relationship

        # test with reversely no equal
        reversely_value = ttypes.Value(eVal=self.get_edge_value(b"Tom", b"Lily", True))
        reversely_value_wrapper = ValueWrapper(reversely_value)
        reversely_relationship = reversely_value_wrapper.as_relationship()
        assert isinstance(reversely_relationship, Relationship)
        assert reversely_relationship != relationship

        relationship.ranking() == 100
        relationship.edge_name() == "classmate"
        relationship.start_vertex_id().as_string() == "Lily"
        relationship.start_vertex_id().as_string() == "Tom"
        assert relationship.keys() == ["prop0", "prop1", "prop2", "prop3", "prop4"]
        expect_values = [(v.as_int()) for v in relationship.values()]
        assert expect_values == [0, 1, 2, 3, 4]
        assert (
            list(relationship.properties().keys()).sort()
            == ["prop0", "prop1", "prop2", "prop3", "prop4"].sort()
        )
        expect_values = [(v.as_int()) for v in relationship.properties().values()]
        assert expect_values == [0, 1, 2, 3, 4]

        # test empty props
        value = ttypes.Value(
            eVal=self.get_edge_value(b"Tom", b"Lily", empty_props=True)
        )
        relationship = ValueWrapper(value).as_relationship()
        assert relationship.keys() == []
        assert relationship.values() == []
        assert len(relationship.properties()) == 0

    def test_as_path(self):
        value = ttypes.Value()
        value.set_pVal(self.get_path_value(b"Tom"))
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_path()

        node = value_wrapper.as_path()
        assert isinstance(node, PathWrapper)

    def test_as_geography(self):
        value = ttypes.Value()
        value.set_ggVal(self.get_geography_value(3.0, 5.2))
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_geography()

        geog = value_wrapper.as_geography()
        assert isinstance(geog, GeographyWrapper)

    def test_as_duration(self):
        value = ttypes.Value()
        value.set_duVal(Duration(86400, 3000, 12))
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_duration()

        duration = value_wrapper.as_duration()
        assert isinstance(duration, DurationWrapper)
        assert str(duration) == "P12MT86400.003000000S"


class TestNode(TestBaseCase):
    def test_node_api(self):
        test_set = set()
        test_set.add(Value())
        node = Node(self.get_vertex_value(b"Tom"))
        assert "Tom" == node.get_id().as_string()

        assert node.has_tag("tag2")

        assert ["prop0", "prop1", "prop2", "prop3", "prop4"] == node.prop_names("tag2")

        assert [0, 1, 2, 3, 4] == [
            (value.as_int()) for value in node.prop_values("tag2")
        ]

        assert ["tag0", "tag1", "tag2"] == node.tags()

        expect_properties = {}
        for key in node.properties("tag2").keys():
            expect_properties[key] = node.properties("tag2")[key].as_int()
        assert {
            "prop0": 0,
            "prop1": 1,
            "prop2": 2,
            "prop3": 3,
            "prop4": 4,
        } == expect_properties


class TestRelationship(TestBaseCase):
    def test_relationship_api(self):
        relationship = Relationship(self.get_edge_value(b"Tom", b"Lily"))

        assert "Tom" == relationship.start_vertex_id().as_string()

        assert "Lily" == relationship.end_vertex_id().as_string()

        assert 100 == relationship.ranking()

        assert 100 == relationship.ranking()

        assert "classmate" == relationship.edge_name()

        assert ["prop0", "prop1", "prop2", "prop3", "prop4"] == relationship.keys()

        expect_properties = {}
        for key in relationship.properties().keys():
            expect_properties[key] = relationship.properties()[key].as_int()
        assert {
            "prop0": 0,
            "prop1": 1,
            "prop2": 2,
            "prop3": 3,
            "prop4": 4,
        } == expect_properties


class TestPath(TestBaseCase):
    def test_path_api(self):
        path = PathWrapper(self.get_path_value(b"Tom"))
        assert Node(self.get_vertex_value(b"Tom")) == path.start_node()

        assert 5 == path.length()

        assert path.contain_node(Node(self.get_vertex_value(b"vertex3")))

        assert path.contain_relationship(
            Relationship(self.get_edge_value(b"vertex3", b"vertex2"))
        )

        nodes = list()
        nodes.append(path.start_node())
        for i in range(0, 5):
            nodes.append(
                Node(self.get_vertex_value(("vertex".format()).encode("utf-8")))
            )

        relationships = list()
        relationships.append(Relationship(self.get_edge_value(b"Tom", b"vertex0")))
        for i in range(0, 4):
            if i % 2 == 0:
                relationships.append(
                    Relationship(
                        self.get_edge_value(
                            ("vertex{}".format(i + 1)).encode("utf-8"),
                            ("vertex{}".format(i)).encode("utf-8"),
                        )
                    )
                )
            else:
                relationships.append(
                    Relationship(
                        self.get_edge_value(
                            ("vertex{}".format(i)).encode("utf-8"),
                            ("vertex{}".format(i + 1)).encode("utf-8"),
                        )
                    )
                )

        assert relationships == path.relationships()


class TestDatesetWrapper(TestBaseCase):
    def test_all(self):
        data_set_wrapper1 = DataSetWrapper(self.get_data_set())
        data_set_wrapper2 = DataSetWrapper(self.get_data_set())

        # test iterator and compare
        row_count = 0
        for i in range(data_set_wrapper1.get_row_size()):
            row_count = row_count + 1
            assert (
                data_set_wrapper1.row_values(i)[0] == data_set_wrapper2.row_values(i)[0]
            )
            assert (
                data_set_wrapper1.row_values(i)[1] == data_set_wrapper2.row_values(i)[1]
            )
            assert (
                data_set_wrapper1.row_values(i)[2] == data_set_wrapper2.row_values(i)[2]
            )
            assert (
                data_set_wrapper1.row_values(i)[3] == data_set_wrapper2.row_values(i)[3]
            )
            assert (
                data_set_wrapper1.row_values(i)[4] == data_set_wrapper2.row_values(i)[4]
            )
            assert (
                data_set_wrapper1.row_values(i)[5] == data_set_wrapper2.row_values(i)[5]
            )
            assert (
                data_set_wrapper1.row_values(i)[6] == data_set_wrapper2.row_values(i)[6]
            )
            assert (
                data_set_wrapper1.row_values(i)[7] == data_set_wrapper2.row_values(i)[7]
            )
            assert (
                data_set_wrapper1.row_values(i)[8] == data_set_wrapper2.row_values(i)[8]
            )
            assert (
                data_set_wrapper1.row_values(i)[9] == data_set_wrapper2.row_values(i)[9]
            )
            assert (
                data_set_wrapper1.row_values(i)[10]
                == data_set_wrapper2.row_values(i)[10]
            )
            assert (
                data_set_wrapper1.row_values(i)[11]
                == data_set_wrapper2.row_values(i)[11]
            )
            assert (
                data_set_wrapper1.row_values(i)[12]
                == data_set_wrapper2.row_values(i)[12]
            )
            assert (
                data_set_wrapper1.row_values(i)[13]
                == data_set_wrapper2.row_values(i)[13]
            )
            assert (
                data_set_wrapper1.row_values(i)[14]
                == data_set_wrapper2.row_values(i)[14]
            )
            assert (
                data_set_wrapper1.row_values(i)[15]
                == data_set_wrapper2.row_values(i)[15]
            )
            assert (
                data_set_wrapper1.row_values(i)[16]
                == data_set_wrapper2.row_values(i)[16]
            )
            assert (
                data_set_wrapper1.row_values(i)[9] != data_set_wrapper2.row_values(i)[8]
            )

        assert 2 == row_count
        assert 2 == data_set_wrapper1.get_row_size()
        assert len(data_set_wrapper1.column_values("col6_string")) == 2
        assert data_set_wrapper1.column_values("col6_string")[0].is_string()
        assert (
            data_set_wrapper1.column_values("col6_string")[0].as_string()
            == "hello world"
        )
        assert (
            data_set_wrapper1.column_values("col6_string")[1].as_string()
            == "hello world"
        )

        assert data_set_wrapper1.row_values(0)[5].is_string()
        assert data_set_wrapper1.row_values(1)[5].is_string()
        assert data_set_wrapper1.row_values(0)[5].as_string() == "hello world"
        assert data_set_wrapper1.row_values(1)[5].as_string() == "hello world"


class TestResultset(TestBaseCase):
    def test_all_interface(self):
        result = self.get_result_set()
        assert result.space_name() == "test"
        assert result.comment() == "Permission"
        assert result.error_msg() == "Permission"
        assert result.error_code() == ErrorCode.E_BAD_PERMISSION
        assert result.plan_desc() is None
        assert result.latency() == 100
        assert not result.is_empty()
        assert not result.is_succeeded()
        expect_keys = [
            "col1_empty",
            "col2_null",
            "col3_bool",
            "col4_int",
            "col5_double",
            "col6_string",
            "col7_list",
            "col8_set",
            "col9_map",
            "col10_time",
            "col11_date",
            "col12_datetime",
            "col13_vertex",
            "col14_edge",
            "col15_path",
            "col16_geography",
            "col17_duration",
        ]
        assert result.keys() == expect_keys
        assert result.col_size() == 17
        assert result.row_size() == 2

        # test column_values
        assert len(result.column_values("col6_string")) == 2
        assert result.column_values("col6_string")[0].is_string()
        assert result.column_values("col6_string")[0].as_string() == "hello world"
        # test row_values
        assert len(result.row_values(0)) == 17
        assert result.row_values(0)[5].is_string()
        assert result.row_values(0)[5].as_string() == "hello world"

        # test rows
        assert len(result.rows()) == 2
        assert len(result.rows()[0].values) == 17
        assert isinstance(result.rows()[0].values[0], Value)
        assert isinstance(result.get_row_types(), list)

        # test get_row_types
        assert result.get_row_types() == [
            ttypes.Value.__EMPTY__,
            ttypes.Value.NVAL,
            ttypes.Value.BVAL,
            ttypes.Value.IVAL,
            ttypes.Value.FVAL,
            ttypes.Value.SVAL,
            ttypes.Value.LVAL,
            ttypes.Value.UVAL,
            ttypes.Value.MVAL,
            ttypes.Value.TVAL,
            ttypes.Value.DVAL,
            ttypes.Value.DTVAL,
            ttypes.Value.VVAL,
            ttypes.Value.EVAL,
            ttypes.Value.PVAL,
            ttypes.Value.GGVAL,
            ttypes.Value.DUVAL,
        ]

        # test record
        in_use = False
        for record in result:
            in_use = True
            record.size() == 17

            # test keys()
            assert record.keys() == expect_keys
            # test values()
            values = record.values()
            assert len(record.values()) == 17
            assert record.values()[0].is_empty()
            assert record.values()[5].is_string()
            assert record.values()[5].is_string()
            assert record.values()[5].as_string() == "hello world"

            # test get_value()
            assert record.get_value(0).is_empty()
            assert values[0].is_empty()
            assert record.get_value(1).is_null()
            assert record.get_value(1).as_null() == Null(Null.BAD_DATA)
            null_value = Value(nVal=Null.BAD_DATA)
            assert record.get_value(1) == ValueWrapper(null_value)
            assert str(record.get_value(1).as_null()) == "BAD_DATA"

            # test get_value_by_key()
            assert record.get_value_by_key("col2_null").is_null()
            assert record.get_value_by_key("col3_bool").is_bool()
            assert not record.get_value_by_key("col3_bool").as_bool()

            # get_value_by_key with not exited key
            try:
                record.get_value_by_key("not existed")
                assert False, "Not expect here"
            except InvalidKeyException as e:
                assert True
                assert e.message == "KeyError: `not existed'"
            assert values[1].is_null()
            assert record.get_value(2).is_bool()
            assert not record.get_value(2).as_bool()
            assert record.get_value(2).is_bool()
            assert record.get_value(3).is_int()
            assert record.get_value(3).as_int() == 100
            assert record.get_value(4).is_double()
            assert record.get_value(4).as_double() == 10.01
            assert record.get_value(5).is_string()
            assert record.get_value(5).as_string() == "hello world"
            assert record.get_value(6).is_list()
            assert record.get_value(7).is_set()
            assert record.get_value(8).is_map()
            assert record.get_value(9).is_time()
            assert record.get_value(10).is_date()
            assert record.get_value(11).is_datetime()
            assert record.get_value(12).is_vertex()
            assert record.get_value(13).is_edge()
            assert record.get_value(14).is_path()
            assert record.get_value(15).is_geography()
            assert record.get_value(16).is_duration()
        assert in_use

        # test use iterator again
        in_use = False
        for record in result:
            in_use = True
            record.size() == 17
        assert in_use
