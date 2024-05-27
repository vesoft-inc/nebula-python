#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


from nebula3.common.ttypes import ErrorCode

from nebula3.data.DataObject import DataSetWrapper, Node, Relationship, PathWrapper


class ResultSet(object):
    def __init__(
        self, resp, all_latency, decode_type='utf-8', timezone_offset: int = 0
    ):
        """Constructor method

        :param resp: the response from the service
        :param all_latency: the execution time from the time the client
        sends the request to the time the response is received and the data is decoded
        :param decode_type: the decode_type for decode binary value, it comes from the service,
        but now the service not return, use utf-8 default
        :param timezone_offset: the timezone offset for calculate local time,
        it comes from the service
        """
        self._decode_type = decode_type
        self._resp = resp
        self._data_set_wrapper = None
        self._all_latency = all_latency
        self._timezone_offset = timezone_offset
        if self._resp.data is not None:
            self._data_set_wrapper = DataSetWrapper(
                data_set=resp.data,
                decode_type=self._decode_type,
                timezone_offset=self._timezone_offset,
            )

    def is_succeeded(self):
        """check the response from the service is succeeded

        :return: bool
        """
        return self._resp.error_code == ErrorCode.SUCCEEDED

    def error_code(self):
        """if the response is failed, the service return the error code

        :return: nebula3.common.ttypes.ErrorCode
        """
        return self._resp.error_code

    def space_name(self):
        """get the space for the current operation

        :return: space name or ''
        """
        if self._resp.space_name is None:
            return ''
        return self._resp.space_name.decode(self._decode_type)

    def error_msg(self):
        """if the response is failed, the service return the error message

        :return: error message
        """
        if self._resp.error_msg is None:
            return ''
        return self._resp.error_msg.decode(self._decode_type)

    def comment(self):
        """the comment return by service, it maybe some warning message

        :return: comment message
        """
        if self._resp.error_msg is None:
            return ''
        return self._resp.comment.decode(self._decode_type)

    def latency(self):
        """the time the server processes the request

        :return: latency
        """
        return self._resp.latency_in_us

    def whole_latency(self):
        """the execution time from the time the client
        sends the request to the time the response is received and the data is decoded

        :return: all_latency
        """
        return self._all_latency

    def plan_desc(self):
        """get plan desc, whe user want to get the execute plan use `PROFILE` and `EXPLAIN`

        :return:plan desc
        """
        return self._resp.plan_desc

    def is_empty(self):
        """the data of response is empty

        :return: true of false
        """
        return (
            self._data_set_wrapper is None or self._data_set_wrapper.get_row_size() == 0
        )

    def keys(self):
        """get the column names

        :return: column names
        """
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.get_col_names()

    def row_size(self):
        """get the row size

        :return: row size
        """
        if self._data_set_wrapper is None:
            return 0
        return len(self._data_set_wrapper.get_rows())

    def col_size(self):
        """get column size

        :return: column size
        """
        if self._data_set_wrapper is None:
            return 0
        return len(self._data_set_wrapper.get_col_names())

    def get_row_types(self):
        """get the value type of the row

        :return: list<int>
          ttypes.Value.__EMPTY__ = 0
          ttypes.Value.NVAL = 1
          ttypes.Value.BVAL = 2
          ttypes.Value.IVAL = 3
          ttypes.Value.FVAL = 4
          ttypes.Value.SVAL = 5
          ttypes.Value.DVAL = 6
          ttypes.Value.TVAL = 7
          ttypes.Value.DTVAL = 8
          ttypes.Value.VVAL = 9
          ttypes.Value.EVAL = 10
          ttypes.Value.PVAL = 11
          ttypes.Value.LVAL = 12
          ttypes.Value.MVAL = 13
          ttypes.Value.UVAL = 14
          ttypes.Value.GVAL = 15
          ttypes.Value.GGVAL = 16
          ttypes.Value.DUVAL = 17
        """
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.get_row_types()

    def row_values(self, row_index):
        """get row values

        :param row_index:
        :return: list<ValueWrapper>
        """
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.row_values(row_index)

    def column_values(self, key):
        """get column values

        :param key: the specified column name
        :return: list<ValueWrapper>
        """
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.column_values(key)

    def rows(self):
        """get all rows

        :return: list<Row>
        """
        if self._data_set_wrapper is None:
            return []
        return self._data_set_wrapper.get_rows()

    def as_primitive(self):
        """Convert result set to list of dict with primitive values per row

        :return: list<dict>
        """
        return [
            {
                col_key: self.row_values(row_index)[col_index].cast_primitive()
                for col_index, col_key in enumerate(self.keys())
            }
            for row_index in range(self.row_size())
        ]

    def dict_for_vis(self):
        """Convert result set to a dictionary format suitable for visualization.

        Example:
        {
            'nodes': [
                {
                    'id': 'player100',
                    'labels': ['player'],
                    'props': {
                        'name': 'Tim Duncan',
                        'age': '42',
                        'id': 'player100'
                    }
                },
                {
                    'id': 'player101',
                    'labels': ['player'],
                    'props': {
                        'age': '36',
                        'name': 'Tony Parker',
                        'id': 'player101'
                    }
                }
            ],
            'edges': [
                {
                    'src': 'player100',
                    'dst': 'player101',
                    'name': 'follow',
                    'props': {
                        'degree': '95'
                    }
                }
            ],
            'nodes_dict': {
                'player100': {
                    'id': 'player100',
                    'labels': ['player'],
                    'props': {
                        'name': 'Tim Duncan',
                        'age': '42',
                        'id': 'player100'
                    }
                },
                'player101': {
                    'id': 'player101',
                    'labels': ['player'],
                    'props': {
                        'age': '36',
                        'name': 'Tony Parker',
                        'id': 'player101'
                    }
                }
            },
            'edges_dict': {
                "('player100', 'player101', 0, 'follow')": {
                    'src': 'player100',
                    'dst': 'player101',
                    'name': 'follow',
                    'props': {
                        'degree': '95'
                    }
                }
            },
            'nodes_count': 2,
            'edges_count': 1
        }

        :return: dict with keys:
            nodes, edges, nodes_dict, edges_dict, nodes_count, edges_count
        """

        def add_to_nodes_or_edges(nodes_dict, edges_dict, item):
            if isinstance(item, Node):
                node_id = str(item.get_id().cast())
                tags = item.tags()  # list of strings
                props_raw = dict()
                for tag in tags:
                    # TODO: handle duplicate keys among tags
                    props_raw.update(item.properties(tag))
                props = {
                    k: str(v.cast()) if hasattr(v, "cast") else str(v)
                    for k, v in props_raw.items()
                }

                if "id" not in props:
                    props["id"] = node_id

                if node_id not in nodes_dict:
                    nodes_dict[node_id] = {
                        "id": node_id,
                        "labels": tags,
                        "props": props,
                    }
                else:
                    nodes_dict[node_id]["labels"] = list(
                        set(nodes_dict[node_id]["labels"] + tags)
                    )
                    nodes_dict[node_id]["props"].update(props)

            elif isinstance(item, Relationship):
                src_id = str(item.start_vertex_id().cast())
                dst_id = str(item.end_vertex_id().cast())
                rank = item.ranking()
                edge_name = item.edge_name()
                props_raw = item.properties()
                props = {
                    k: str(v.cast()) if hasattr(v, "cast") else str(v)
                    for k, v in props_raw.items()
                }
                if str((src_id, dst_id, rank, edge_name)) not in edges_dict:
                    edges_dict[str((src_id, dst_id, rank, edge_name))] = {
                        "src": src_id,
                        "dst": dst_id,
                        "name": edge_name,
                        "rank": rank,
                        "props": props,
                    }
                else:
                    edges_dict[str((src_id, dst_id, rank, edge_name))]["props"].update(
                        props
                    )

            elif isinstance(item, PathWrapper):
                for node in item.nodes():
                    add_to_nodes_or_edges(nodes_dict, edges_dict, node)
                for edge in item.relationships():
                    add_to_nodes_or_edges(nodes_dict, edges_dict, edge)

            elif isinstance(item, list):
                for it in item:
                    add_to_nodes_or_edges(nodes_dict, edges_dict, it)

        nodes_dict = dict()
        edges_dict = dict()

        columns = self.keys()
        for col_num in range(self.col_size()):
            col_name = columns[col_num]
            col_list = self.column_values(col_name)
            add_to_nodes_or_edges(nodes_dict, edges_dict, [x.cast() for x in col_list])
        nodes = list(nodes_dict.values())
        edges = list(edges_dict.values())
        # move rank to props, omit rank 0
        for edge in edges:
            if "rank" in edge:
                rank = edge.pop("rank")
                if rank != 0:
                    edge["props"]["rank"] = rank

        return {
            "nodes": nodes,
            "edges": edges,
            "nodes_dict": nodes_dict,
            "edges_dict": edges_dict,
            "nodes_count": len(nodes),
            "edges_count": len(edges),
        }

    def as_data_frame(self, primitive: bool = True):
        """Convert result set to a DataFrame.

        :param primitive: if True, convert all values to primitive types
        :return: DataFrame
        """
        # TODO: support polars df
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is not installed")

        if self.is_empty():
            return pd.DataFrame()

        data = dict()
        for col in self.keys():
            if primitive:
                data[col] = [x.cast_primitive() for x in self.column_values(col)]
            else:
                data[col] = [x.cast() for x in self.column_values(col)]

        return pd.DataFrame(data)

    def __iter__(self):
        """the iterator for per row

        :return: iter
        """
        return iter(self._data_set_wrapper)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self._data_set_wrapper)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
