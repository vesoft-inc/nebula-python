#!/usr/bin/env python

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
import logging


def create_space(client, space):
    pass

def create_tag_schemas(client, tag_schemas):
    for tag_name, tag_schema in tag_schemas.items():
        command = "create TAG "+tag_name
        [ name+" "+type for name, type in tag_schema.items()]



def create_edge_schemas(client, edge_schemas):
    pass


def insert_vertexs(client, data):
    pass


def insert_edges(client, data):
    pass


def query():
    pass

    


def main(host, port):
    client = GraphClient(host, port)

    '''
    Connection using default user and password
    '''
    client.authenticate("user", "password")

    print("CREATE SPACE {}(PARTITION_NUM = 1, REPLICA_FACTOR = 1)".format("test"))
    
    client.execute("USE t")

    tag_schemas = {
        "course": {"name": "string", "credits": "int"},
        "building": {"name": "string"},
        "student": {"name": "string", "age": "int", "gender": "string"}
    }
    create_tag_schemas(client, tag_schemas)

    edge_schemas = {
        "like": {"likeness": "double"},
        "select": {"grade": "int"}
    }
    create_edge_schemas(client, edge_schemas)

    student_vertices_data = {
        200: {"name": "Monica", "age": 16, "gender": "female"},
        200: {"name": "Mike", "age": 18, "gender": "male"},
        200: {"name": "Jane", "age": 17, "gender": "female"},
    }

    course_vertices_data = {
        101: {"name": "Math", "credits": 3},
        102: {"name": "English", "credits": 6}
    }

    building_vertices_data = {
        101: {"name": "No5"},
        102: {"name": "No11"}
    }
    insert_vertexs(client, student_vertices_data)
    insert_vertexs(client, course_vertices_data)
    insert_vertexs(client, building_vertices_data)

    select_edges_data = {
        (200, 101): {"grade": 5},
        (200, 102): {"grade": 3},
        (201, 102): {"grade": 3},
        (202, 102): {"grade": 3},
    }

    like_edges_data = {
        (200, 201): {"likeness": 92.5},
        (201, 200): {"likeness": 85.6},
        (201, 202): {"likeness": 93.2},
    }
    insert_edges(client, select_edges_data);
    insert_edges(client, like_edges_data);

    client.signout()


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: python example.py [host] [port]')

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    main(sys.argv[1], sys.argv[2])
