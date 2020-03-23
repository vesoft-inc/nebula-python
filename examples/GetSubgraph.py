# --coding:utf-8--
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

"""
Nebula Client example.
"""

import sys
import time
import concurrent.futures
import traceback
from concurrent.futures import ThreadPoolExecutor
from graph import ttypes
from nebula.ConnectionPool import ConnectionPool
from nebula.Client import GraphClient, ExecutionException


def add_src_dst(resp):
    vertex_list = set()
    src_dst_list = []
    if resp.rows is None:
        print('rows is empty')
        return True, {}, []
    for row in resp.rows:
        src_dst = []
        for col in row.columns:
            if col.getType() != ttypes.ColumnValue.ID:
                print('ERROR: type is wrong')
                return False, {}, []
            src_dst.append(col.get_id())
            vertex_list.add(col.get_id())
        src_dst_list.append(src_dst)
    return True, vertex_list, src_dst_list


def get_edge(client, edge, id_str, reversely):
    cmd = ''
    if reversely:
        cmd = 'GO FROM ' + id_str + ' OVER ' + edge + ' REVERSELY ' + \
              ' YIELD ' + edge + '._src AS src, ' + edge + '._dst AS dst'
    else:
        cmd = 'GO FROM ' + id_str + ' OVER ' + edge + \
              ' YIELD ' + edge + '._src AS src, ' + edge + '._dst AS dst'
    # print(cmd)
    resp = client.execute_query(cmd)
    ret, vertex_list, src_dst = add_src_dst(resp)
    return ret, edge, vertex_list, src_dst


def get_srcId_dstId(clients, id_list, edge_list):
    edge_src_dst = {}
    vertex_list = set()
    if len(id_list) == 0:
        return True, {}, {}

    reqs = []
    for edge in edge_list:
        id_str = ''
        for vid in id_list:
            id_str = id_str + str(vid) + ','
        if len(id_str) != 0:
            id_str = id_str[:-1]
        reqs.append([edge, id_str, False])
        reqs.append([edge, id_str, True])

    num = len(reqs)
    with concurrent.futures.ThreadPoolExecutor(num) as executor:
        to_do = []
        for i, req in zip(range(0, num),reqs):
            future = executor.submit(get_edge, clients[i], req[0], req[1], req[2])
            to_do.append(future)
        # future.add_done_callback(over)
        for future in concurrent.futures.as_completed(to_do):
            if future.exception() is not None:
                print(future.exception())
                return False, {}, []
            else:
                ret, edge, vertex, src_dst = future.result()
                if not ret:
                    continue
                vertex_list.update(vertex)
                if edge in edge_src_dst:
                    edge_src_dst[edge] = edge_src_dst[edge] + src_dst
                else:
                    edge_src_dst[edge] = src_dst

    return True, edge_src_dst, vertex_list


def get_vertex_props(client, id_str, tag_name):
    cmd = 'FETCH PROP ON ' + tag_name + ' ' + id_str
    # print(cmd)
    return client.execute_query(cmd)


def fetch_vertex(clients, id_list, tag_list):
    if len(clients) == 0:
        return False, True, {}
    if len(id_list) == 0 or len(tag_list) == 0:
        return False, True, {}
    id_str = ''
    for vid in id_list:
        id_str = id_str + str(vid) + ','
    id_str = id_str[:-1]

    reqs = []
    for tag in tag_list:
        reqs.append([id_str, tag])

    num = len(reqs)
    vertexes_list = {}
    with concurrent.futures.ThreadPoolExecutor(num) as executor:
        to_do = []
        for i, req in zip(range(0, num),reqs):
            future = executor.submit(get_vertex_props, clients[i], req[0], req[1])
            to_do.append(future)
        for future, req in zip(concurrent.futures.as_completed(to_do), reqs):
            if future.exception() is not None:
                print(future.exception())
                continue
            else:
                resp = future.result()
                if resp.error_code:
                    print('Execute failed: %s' % resp.error_msg)
                    continue
                if not resp.rows:
                    continue
                for row in resp.rows:
                    if len(resp.column_names) != len(row.columns):
                        continue
                    vid = row.columns[0].get_id()
                    name_value_list = {}
                    for name, col in zip(resp.column_names[1:], row.columns[1:]):
                        if col.getType() == ttypes.ColumnValue.__EMPTY__:
                            print('ERROR: type is empty')
                            return False, True, {}
                        elif col.getType() == ttypes.ColumnValue.BOOL_VAL:
                            name_value_list[name] = col.get_bool_val()
                        elif col.getType() == ttypes.ColumnValue.INTEGER:
                            name_value_list[name] = col.get_integer()
                        elif col.getType() == ttypes.ColumnValue.ID:
                            continue
                        elif col.getType() == ttypes.ColumnValue.STR:
                            name_value_list[name] = col.get_str().decode('utf-8')
                        elif col.getType() == ttypes.ColumnValue.DOUBLE_PRECISION:
                            name_value_list[name] = col.get_double_precision()
                        elif col.getType() == ttypes.ColumnValue.TIMESTAMP:
                            name_value_list[name] = col.get_timestamp()
                        else:
                            print('ERROR: Type unsupported')
                            return False, True, {}

                    if len(name_value_list) == 0:
                        continue
                    vertexes_list[vid] = name_value_list
    return True, True, vertexes_list


def get_edge_props(client, id_str, edge_name):
    cmd = 'FETCH PROP ON ' + edge_name + ' ' + id_str
    # print(cmd)
    resp = client.execute_query(cmd)
    return edge_name, resp


def fetch_edges(clients, edge_src_dst_list):
    if len(edge_src_dst_list) == 0:
        return False, False, {}
    edges_list = {}
    reqs = []
    for edge_name in edge_src_dst_list:
        src_dst_list = edge_src_dst_list[edge_name]
        id_str = ''
        for edge in src_dst_list:
            id_str += str(edge[0]) + '->' + str(edge[1]) + ','
        if len(id_str) > 0:
            id_str = id_str[:-1]
        if len(id_str) == 0:
            continue
        reqs.append([id_str, edge_name])

    num = len(reqs)
    resps = {}
    with concurrent.futures.ThreadPoolExecutor(num) as executor:
        to_do = []
        for i, req in zip(range(0, num), reqs):
            future = executor.submit(get_edge_props, clients[i], req[0], req[1])
            to_do.append(future)
        for future, req in zip(concurrent.futures.as_completed(to_do), reqs):
            if future.exception() is not None:
                print(future.exception())
                continue
            else:
                edge_name, resp = future.result()
                if resp.error_code:
                    print('Execute failed: %s' % resps[name].error_msg)
                    exit(1)
                for row in resp.rows:
                    name_value_list = {}
                    src_id = row.columns[0].get_id()
                    dst_id = row.columns[1].get_id()
                    for name, col in zip(resp.column_names[3:], row.columns[3:]):
                        if col.getType() == ttypes.ColumnValue.__EMPTY__:
                            print('ERROR: type is empty')
                            return False, False, {}
                        elif col.getType() == ttypes.ColumnValue.BOOL_VAL:
                            name_value_list[name] = col.get_bool_val()
                        elif col.getType() == ttypes.ColumnValue.INTEGER:
                            name_value_list[name] = col.get_integer()
                        elif col.getType() == ttypes.ColumnValue.ID:
                            name_value_list[name] = name, col.get_id()
                        elif col.getType() == ttypes.ColumnValue.STR:
                            name_value_list[name] = col.get_str().decode('utf-8')
                        elif col.getType() == ttypes.ColumnValue.DOUBLE_PRECISION:
                            name_value_list[name] = col.get_double_precision()
                        elif col.getType() == ttypes.ColumnValue.TIMESTAMP:
                            name_value_list[name] = col.get_timestamp()
                        else:
                            print('ERROR: Type unsupported')
                            return False, False, {}
                    edge_key = (edge_name, src_id, dst_id)
                    edges_list[edge_key] = name_value_list

    return True, False, edges_list


"""get_subgraph: get all vertices and edges and all of their properties in the subgraph
Arguments:
	- client: the graph client
	- id: the start vid
	- steps: want to go n steps
	- tag_list: all tag name in space
	- edge_name_list: all edge name in space
Returns:
	- result: True or False
	- vertices: all vertices in subgraph
	- edges: all edges in subgraph
"""
def get_subgraph(clients, id, steps, tag_list, edge_name_list):
    print('input id: %d, steps: %d' % (id, steps))
    edge_list = {}
    vertex_list = set([id])
    vertices = {}
    id_list = [id]

    for i in range(1, steps+1):
        # start_time = time.time()
        ret, edges, vertices = get_srcId_dstId(clients, id_list, edge_name_list)
        if not ret:
            print('Get subgraph failed')
            return False, {}, {}
        id_list = vertices
        edge_list.update(edges)
        vertex_list.update(vertices)
        # end_time = time.time()
        # print('step {} cost {} seconds'.format(i, end_time - start_time))

    # Gets the relationship between the outermost points
    if vertices:
        # start_time = time.time()
        result, end_edges, end_vertices = get_srcId_dstId(clients, vertices, edge_name_list)
        if not result:
            print('Get subgraph failed')
            return False, {}, {}
        if len(end_edges) != 0:
            for edge_name in end_edges:
                for edge in end_edges[edge_name]:
                    if edge[0] in vertices and edge[1] in vertices:
                        # print('Gets the outermost relation %d -> %d' % (edge[0],edge[1]))
                        edge_list[edge_name].append(edge)
        # end_time = time.time()
        # print('step {} cost {} seconds'.format(steps+1, end_time - start_time))

    # distinct
    for edge in edge_list:
        temp = []
        for item in edge_list[edge]:
            if item not in temp:
                temp.append(item)
        edge_list[edge] = temp
    # start_time = time.time()
    client_num = len(clients)
    mid = client_num/2
    with concurrent.futures.ThreadPoolExecutor(3) as executor:
        to_do = []
        v_future = executor.submit(fetch_vertex, clients[0:int(mid)], vertex_list, tag_list)
        e_future = executor.submit(fetch_edges, clients[int(mid):int(client_num)], edge_list)
        to_do.append(v_future)
        to_do.append(e_future)
        for future in concurrent.futures.as_completed(to_do):
            if future.exception() is not None:
                print(future.exception())
                continue
            else:
                ret, is_vertex,resp = future.result()
                if not ret:
                    continue
                if is_vertex:
                    vertices = resp
                else:
                    edges = resp
    # end_time = time.time()
    # print('fetch props cost {} seconds'.format(end_time - start_time))
    return True, vertices, edges


if __name__ == '__main__':

    try:
        g_ip = '127.0.0.1'
        g_port = 3699

        print('input argv num is %d' % len(sys.argv))
        if len(sys.argv) == 3:
            print('input argv num is 3')
            g_ip = sys.argv[1]
            print('ip: %s' % g_ip)
            g_port = sys.argv[2]
            print('port: %s' % g_port)

        # init connection pool
        # if your tags or edges' num is more than 10,
        # please make sure conn_num >= max(tags_num, edges_num) * 2
        conn_num = 20
        connection_pool = ConnectionPool(g_ip, g_port, conn_num)

        clients = []
        for i in range(0, conn_num):
            client = GraphClient(connection_pool)
            client.authenticate('user', 'password')
            client.execute('USE test')
            clients.append(client)
        # Get client
        resp = clients[0].execute_query('SHOW TAGS')
        if resp.error_code:
            raise ExecutionException('SHOW TAGS failed')

        tag_list = []
        for row in resp.rows:
            tag_list.append(row.columns[1].get_str().decode('utf-8'))

        resp = clients[0].execute_query('SHOW EDGES')
        if resp.error_code:
            raise ExecutionException('SHOW EDGES failed')

        edge_list = []
        for row in resp.rows:
            edge_list.append(row.columns[1].get_str().decode('utf-8'))

        start_time = time.time()

        vid = 100

        result, all_vertices, all_edges = get_subgraph(clients, vid, 2, tag_list, edge_list)

        end_time = time.time()
        print('cost {} seconds'.format(end_time - start_time))

        print('=========== vertices ===========, num: %d' % len(all_vertices))
        print(all_vertices)
        print('============= edges ============, num: %d' % len(all_edges))
        print(all_edges)
        connection_pool.close()

    except Exception as x:
        print(x)
        print('============ traceback =============')
        traceback.print_exc()



