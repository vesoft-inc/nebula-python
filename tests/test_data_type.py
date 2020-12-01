#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
import os
from datetime import date

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.join(current_dir, '..')
sys.path.insert(0, root_dir)

from nebula2.common.ttypes import Value, NullType, Time, DateTime, Set
from nebula2.common import ttypes
from nebula2.graph import ttypes as graphTtype
from unittest import TestCase
from nebula2.data.ResultSet import ResultSet
from nebula2.data.DataObject import (
    ValueWrapper,
    Node,
    Relationship,
    Path
)


class TestBaseCase(TestCase):
    @classmethod
    def get_vertex_value(self, vid):
        vertex = ttypes.Vertex()
        vertex.vid = vid
        vertex.tags = list()
        for i in range(0, 3):
            tag = ttypes.Tag()
            tag.name = ('tag{}'.format(i)).encode('utf-8')
            tag.props = dict()
            for j in range(0, 5):
                value = ttypes.Value()
                value.set_iVal(j)
                tag.props[('prop{}'.format(j)).encode('utf-8')] = value
            vertex.tags.append(tag)
        return vertex

    @classmethod
    def get_edge_value(self, src_id, dst_id):
        edge = ttypes.Edge()
        edge.src = src_id
        edge.dst = dst_id
        edge.type = 1
        edge.name = b'classmate'
        edge.ranking = 100
        edge.props = dict()
        for i in range(0, 5):
            value = ttypes.Value()
            value.set_iVal(i)
            edge.props[('prop{}'.format(i)).encode('utf-8')] = value
        return edge

    @classmethod
    def get_path_value(self, start_id, steps=5):
        path = ttypes.Path()
        path.src = self.get_vertex_value(start_id)
        path.steps = list()
        for i in range(0, steps):
            step = ttypes.Step()
            step.dst = self.get_vertex_value(('vertex{}'.format(i)).encode('utf-8'))
            step.type = 1 if i % 2 == 0 else -1
            step.name = b'classmate'
            step.ranking = 100
            step.props = dict()
            for i in range(0, 5):
                value = ttypes.Value()
                value.set_iVal(i)
                step.props[('prop{}'.format(i)).encode('utf-8')] = value
            path.steps.append(step)
        return path

    @classmethod
    def get_result_set(self):
        resp = graphTtype.ExecutionResponse()
        resp.error_code = graphTtype.ErrorCode.E_BAD_PERMISSION
        resp.error_msg = b"Permission"
        resp.comment = b"Permission"
        resp.space_name = b"test"
        resp.latency_in_us = 100
        data_set = ttypes.DataSet()
        data_set.column_names = [b"col1_empty",
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
                                 b"col15_path"]
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
        value7.set_lVal([str_val1, str_val2])
        row.values.append(value7)
        value8 = ttypes.Value()
        set_val = Set()
        set_val.values = set()
        set_val.values.add(str_val1)
        set_val.values.add(str_val2)
        value8.set_uVal(set_val)
        row.values.append(value8)
        value9 = ttypes.Value()
        value9.set_mVal({b"a": str_val1, b"b": str_val2})
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
        value13.set_vVal(self.get_vertex_value(b"Tom"))
        row.values.append(value13)
        value14 = ttypes.Value()
        value14.set_eVal(self.get_edge_value(b"Tom", b"Lily"))
        row.values.append(value14)
        value15 = ttypes.Value()
        value15.set_pVal(self.get_path_value(b"Tom", 3))
        row.values.append(value15)
        data_set.rows = []
        data_set.rows.append(row)
        resp.data = data_set
        return ResultSet(resp)


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
        value.set_sVal(b'Tom')
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
        value.set_lVal([str_val1, str_val2])
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_list()

        list_val = value_wrapper.as_list()
        assert isinstance(list_val, list)

    def test_as_set(self):
        value = ttypes.Value()
        str_val1 = ttypes.Value()
        str_val1.set_sVal(b"word")
        str_val2 = ttypes.Value()
        str_val2.set_uVal(b"car")
        set_val = Set()
        set_val.values = set()
        set_val.values.add(str_val1)
        set_val.values.add(str_val2)
        value.set_uVal(set_val)
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_set()

        set_val = value_wrapper.as_set()
        assert isinstance(set_val, set)

    def test_as_map(self):
        value = ttypes.Value()
        str_val1 = ttypes.Value()
        str_val1.set_sVal(b"word")
        str_val2 = ttypes.Value()
        str_val2.set_sVal(b"car")
        value.set_mVal({b"a": str_val1, b"b": str_val2})
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_map()

        map_val = value_wrapper.as_map()
        assert isinstance(map_val, dict)

    def test_as_node(self):
        value = ttypes.Value()
        value.set_vVal(self.get_vertex_value(b'Tom'))
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_vertex()

        node = value_wrapper.as_node()
        assert isinstance(node, Node)

    def test_as_relationship(self):
        value = ttypes.Value()
        value.set_eVal(self.get_edge_value(b'Tom', b'Lily'))
        value_wrapper = ValueWrapper(value)
        assert value_wrapper.is_edge()

        relationship = value_wrapper.as_relationship()
        assert isinstance(relationship, Relationship)

    def test_as_path(self):
        value = ttypes.Value()
        value.set_pVal(self.get_path_value(b'Tom'))
        vaue_wrapper = ValueWrapper(value)
        assert vaue_wrapper.is_path()

        node = vaue_wrapper.as_path()
        assert isinstance(node, Path)


class TestNode(TestBaseCase):
    def test_node_api(self):
        test_set = set()
        test_set.add(Value())
        node = Node(self.get_vertex_value(b'Tom'))
        assert 'Tom' == node.get_id()

        assert node.has_tag('tag2')

        assert ['prop0', 'prop1', 'prop2', 'prop3', 'prop4'] == node.prop_names('tag2')

        assert [0, 1, 2, 3, 4] == [(value.as_int()) for value in node.prop_values('tag2')]

        assert ['tag0', 'tag1', 'tag2'] == node.tags()

        expect_propertys = {}
        for key in node.propertys('tag2').keys():
            expect_propertys[key] = node.propertys('tag2')[key].as_int()
        assert {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4} == expect_propertys


class TestRelationship(TestBaseCase):
    def test_relationship_api(self):
        relationship = Relationship(self.get_edge_value(b'Tom', b'Lily'))

        assert 'Tom' == relationship.start_vertex_id()

        assert 'Lily' == relationship.end_vertex_id()

        assert 100 == relationship.ranking()

        assert 100 == relationship.ranking()

        assert 'classmate' == relationship.edge_name()

        assert ['prop0', 'prop1', 'prop2', 'prop3', 'prop4'] == relationship.keys()

        expect_propertys = {}
        for key in relationship.propertys().keys():
            expect_propertys[key] = relationship.propertys()[key].as_int()
        assert {'prop0': 0, 'prop1': 1, 'prop2': 2, 'prop3': 3, 'prop4': 4} == expect_propertys


class TestPath(TestBaseCase):
    def test_path_api(self):
        path = Path(self.get_path_value(b'Tom'))
        assert Node(self.get_vertex_value(b'Tom')) == path.start_node()

        assert 5 == path.length()

        assert path.contain_node(Node(self.get_vertex_value(b'vertex3')))

        assert path.contain_relationship(Relationship(self.get_edge_value(b'vertex3', b'vertex2')))

        nodes = list()
        nodes.append(path.start_node())
        for i in range(0, 5):
            nodes.append(Node(self.get_vertex_value(('vertex'.format(i)).encode('utf-8'))))

        relationships = list()
        relationships.append(Relationship(self.get_edge_value(b'Tom', b'vertex0')))
        for i in range(0, 4):
            if i % 2 == 0:
                relationships.append(Relationship(
                    self.get_edge_value(('vertex{}'.format(i + 1)).encode('utf-8'),
                                        ('vertex{}'.format(i)).encode('utf-8'))))
            else:
                relationships.append(Relationship(
                    self.get_edge_value(('vertex{}'.format(i)).encode('utf-8'),
                                        ('vertex{}'.format(i + 1)).encode('utf-8'))))

        assert relationships == path.relationships()


class TestResultset(TestBaseCase):
    def test_all_interface(self):
        result = self.get_result_set()
        assert result.space_name() == "test"
        assert result.comment() == "Permission"
        assert result.error_msg() == "Permission"
        assert result.error_code() == graphTtype.ErrorCode.E_BAD_PERMISSION
        assert not result.is_empty()
        assert not result.is_succeeded()
        assert result.keys() == ["col1_empty",
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
                                 "col15_path"]
        assert result.col_size() == 15
        assert result.row_size() == 1
        assert len(result.column_values("col6_string")) == 1
        assert len(result.row_values(0)) == 15
        assert len(result.rows()) == 1
        assert isinstance(result.get_row_types(), list)
        assert result.get_row_types() == [ttypes.Value.__EMPTY__,
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
                                          ttypes.Value.PVAL]
