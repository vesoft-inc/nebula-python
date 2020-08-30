# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

"""
Nebula StorageClient example.
"""

import sys, getopt
import networkx as nx
from meta.ttypes import ErrorCode

sys.path.insert(0, '../')

from nebula.ngMeta.MetaClient import MetaClient
from nebula.ngStorage.StorageClient import StorageClient
from nebula.ngStorage.ngProcessor.ScanEdgeProcessor import ScanEdgeProcessor
from nebula.ngStorage.ngProcessor.ScanVertexProcessor import ScanVertexProcessor


def scan_edge(space, return_cols, all_cols):
    scan_edge_response_iter = storage_client.scan_edge(space, return_cols, all_cols, 100, 0, sys.maxsize)
    scan_edge_response = scan_edge_response_iter.next()
    if scan_edge_response is None:
        print('scan_edge_response is None')
        return
    process_edge(space, scan_edge_response)
    while scan_edge_response_iter.has_next():
        scan_edge_response = scan_edge_response_iter.next()
        if scan_edge_response is None:
            print("Error occurs while scaning edge")
            break
        process_edge(space, scan_edge_response)

def scan_vertex(space, return_cols, all_cols):
    scan_vertex_response_iter = storage_client.scan_vertex(space, return_cols, all_cols, 100, 0, sys.maxsize)
    scan_vertex_response = scan_vertex_response_iter.next()
    if scan_vertex_response is None:
        print('scan_vertex_vesponse is None')
        return
    process_vertex(space, scan_vertex_response)
    while scan_vertex_response_iter.has_next():
        scan_vertex_response = scan_vertex_response_iter.next()
        if scan_vertex_response is None:
            print("Error occurs while scaning vertex")
            break
        process_vertex(space, scan_vertex_response)

def process_edge(space, scan_edge_response):
    result = scan_edge_processor.process(space, scan_edge_response)
    # Get the corresponding rows by edgeName
    for edge_name, edge_rows in result._rows.items():
        for row in edge_rows:
            srcId = row._default_properties[0].get_value()
            dstId = row._default_properties[2].get_value()
            print(srcId,'->', dstId)
            props = {}
            for prop in row._properties:
                prop_name = prop.get_name()
                prop_value = prop.get_value()
                props[prop_name] = prop_value
            print(props)
            # add edge and its properties to Graph G in NetworkX
            G.add_edges_from([(srcId, dstId, props)])

def process_vertex(space, scan_vertex_response):
    result = scan_vertex_processor.process(space, scan_vertex_response)
    if result is None:
        return None
    for tag_name, tag_rows in result._rows.items():
        for row in tag_rows:
            vid = row._default_properties[0].get_value()
            props = {}
            for prop in row._properties:
                prop_name = prop.get_name()
                prop_value = prop.get_value()
                props[prop_name] = prop_value
            print(props)
            # add node and its properties to Graph G in NetworkX
            G.add_nodes_from([(vid, props)])

def get_return_cols(space):
    tag_items = meta_client.get_tags(space)
    vertex_return_cols = {}
    if tag_items is None:
        print('tags not found in space ', space)
    else:
        for tag_item in tag_items:
            tag_name = tag_item.tag_name
            vertex_return_cols[tag_name] = meta_client.get_tag_schema(space, tag_name).keys()
    edge_items = meta_client.get_edges(space)
    edge_return_cols = {}
    if edge_items is None:
        print('edges not found in space ', space)
    else:
        for edge_item in edge_items:
            edge_name = edge_item.edge_name
            edge_return_cols[edge_name] = meta_client.get_edge_schema(space, edge_name).keys()

    return vertex_return_cols, edge_return_cols


if __name__ == '__main__':
    # initialize a MetaClient to establish a connection with the meta server
    meta_client = MetaClient([(sys.argv[1], sys.argv[2])])
    code =  meta_client.connect()
    if code == ErrorCode.E_FAIL_TO_CONNECT:
        raise Exception('connect to %s:%d failed' % (sys.argv[1], sys.argv[2]))
    # initialize a StorageClient
    storage_client = StorageClient(meta_client)
    # initialize a ScanEdgeProcessor to process scanned edge data
    scan_edge_processor = ScanEdgeProcessor(meta_client)
    # initialize a ScanVertexProcessor to process scanned vertex data
    scan_vertex_processor = ScanVertexProcessor(meta_client)

    space_to_read = sys.argv[3]
    # get argument return_cols, which is used in function scan_edge, scan_vertex, scan_part_edge, scan_part_vertex
    vertex_return_cols, edge_return_cols = get_return_cols(space_to_read)
    all_cols = True

    # initialize a Graph in NetworkX
    G = nx.Graph()
    if space_to_read not in meta_client.get_parts_alloc_from_cache().keys():
        raise Exception('spaceToRead %s is not found in nebula' % space_to_read)
    else:
        # scan vertex data
        scan_vertex(space_to_read, vertex_return_cols, all_cols)
        # scan edge data
        scan_edge(space_to_read, edge_return_cols, all_cols)
