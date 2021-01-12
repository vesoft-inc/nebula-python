# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

from nebula.graph import ttypes
from nebula.ConnectionPool import ConnectionPool
from nebula.Client import GraphClient

statement_file = 'nebula_statments.txt'


class Generator(object):
    def __init__(self):
        connection_pool = ConnectionPool(options.ip, options.port, 1, 0)
        self._client = GraphClient(connection_pool)
        self._client.authenticate(options.user, options.password)
        self._spaces = []
        self._stmts = []

    def start(self):
        if options.type != 'config':
            self.generate_space_statment()
        if options.type == 'all' or options.type == 'config':
            self.generate_configs_statment()

    def generate_space_statment(self):
        resp = self.execute("SHOW SPACES;")

        for row in resp.rows:
            self._spaces.append(row.columns[0].get_str().decode('utf-8'))

        if len(self._spaces) == 0:
            return

        for space in self._spaces:
            self._stmts.append('\n')
            self._stmts.append('#  ============ Space[{}] ========='.format(space))
            if options.type == 'all' or options.type == 'space':
                resp = self.execute("SHOW CREATE SPACE {};".format(space))
                self._stmts.append(resp.rows[0].columns[1].get_str().decode('utf-8').replace('\n', ''))

            if options.type == 'space':
                continue

            self.execute("USE {};".format(space))
            self._stmts.append('USE {};'.format(space))

            if options.type == 'all' or options.type == 'schema':
                self.generate_tag_statment()
                self.generate_edge_statment()
            if options.type == 'all' or options.type == 'index':
                self.generate_tag_index_statment()
                self.generate_edge_index_statment()

    def generate_tag_statment(self):
        resp = self.execute("SHOW TAGS;")

        tags = []
        for row in resp.rows:
            tags.append(row.columns[1].get_str().decode('utf-8'))

        if len(tags) == 0:
            return

        for tag in tags:
            resp = self.execute("SHOW CREATE TAG {};".format(tag))
            self._stmts.append(resp.rows[0].columns[1].get_str().decode('utf-8').replace('\n', '') + ';')

    def generate_edge_statment(self):
        resp = self.execute("SHOW EDGES;")

        edges = []
        for row in resp.rows:
            edges.append(row.columns[1].get_str().decode('utf-8'))

        if len(edges) == 0:
            return

        for edge in edges:
            resp = self.execute("SHOW CREATE EDGE {};".format(edge))
            self._stmts.append(resp.rows[0].columns[1].get_str().decode('utf-8').replace('\n', '') + ';')

    def generate_tag_index_statment(self):
        resp = self.execute("SHOW TAG INDEXES;")

        tag_indexes = []
        for row in resp.rows:
            tag_indexes.append(row.columns[1].get_str().decode('utf-8'))

        if len(tag_indexes) == 0:
            return

        for index in tag_indexes:
            resp = self.execute("SHOW CREATE TAG INDEX {};".format(index))
            self._stmts.append(resp.rows[0].columns[1].get_str().decode('utf-8').replace('\n', '') + ';')

    def generate_edge_index_statment(self):
        resp = self.execute("SHOW EDGE INDEXES;")

        edge_indexes = []
        for row in resp.rows:
            edge_indexes.append(row.columns[1].get_str().decode('utf-8'))

        if len(edge_indexes) == 0:
            return

        for index in edge_indexes:
            resp = self.execute("SHOW CREATE EDGE INDEX {};".format(index))
            self._stmts.append(resp.rows[0].columns[1].get_str().decode('utf-8').replace('\n', '') + ';')

    def generate_configs_statment(self):
        resp = self.execute("SHOW CONFIGS;")

        # moduleName, configName, value
        configs = []
        for row in resp.rows:
            module = row.columns[0].get_str().decode('utf-8')
            config = row.columns[1].get_str().decode('utf-8')
            col_val = row.columns[4]
            if col_val.getType() == ttypes.ColumnValue.BOOL_VAL:
                configs.append((module, config, col_val.get_bool_val()))
            elif col_val.getType() == ttypes.ColumnValue.INTEGER:
                configs.append((module, config, col_val.get_integer()))
            elif col_val.getType() == ttypes.ColumnValue.STR:
                configs.append((module, config,
                                col_val.get_str().decode('utf-8').
                                replace('\n', '').
                                replace(':', '=').
                                replace('"', '') + ';'))
            elif col_val.getType() == ttypes.ColumnValue.DOUBLE_PRECISION:
                configs.append((module, config, col_val.get_double_precision()))
            else:
                print("ERROR: Config {}:{} type `{}' unsupported".format(
                    module, config, col_val, col_val.getType()))
                exit(1)

        if len(configs) == 0:
            return

        self._stmts.append('\n')
        self._stmts.append('#  ============ Configs =========')
        for config in configs:
            self._stmts.append('UPDATE CONFIGS {}:{} = {}'.format(config[0], config[1], config[2]))

    def execute(self, stmt):
        resp = self._client.execute_query(stmt)
        if resp.error_code != ttypes.ErrorCode.SUCCEEDED:
            print("Execute `SHOW SPACES' failed: ".format(resp.error_msg))
            exit(1)
        return resp

    def print(self):
        for stmt in self._stmts:
            print(stmt)

    def save_to_file(self):
        f = open(statement_file, 'w')
        for stmt in self._stmts:
            f.write(stmt + '\n')
        f.close()
        print('The generated nGQLs file is ./{}'.format(statement_file))


if __name__ == '__main__':
    '''
        Usage: 
        Step1: pip3 install nebula-python
        Step2: python3 GenerateNgql.py --ip=127.0.0.1 --port=3699 --user=root --password=nebula --type=<all/index/schema>
        You can find a file Nebula_Statments.txt under current directory
    '''
    from optparse import OptionParser
    opt_parser = OptionParser()
    opt_parser.add_option("--ip",
                          dest='ip',
                          default='',
                          help='ip of the graphd')
    opt_parser.add_option("--port",
                          dest='port',
                          default='',
                          help='port of the graphd')
    opt_parser.add_option("--user",
                          dest='user',
                          default='root',
                          help='the user name to authenticate')
    opt_parser.add_option("--password",
                          dest='password',
                          default='',
                          help='the user name to authenticate')
    opt_parser.add_option("--type",
                          dest='type',
                          default='all',
                          help='generate the ngql from given type')

    (options, args) = opt_parser.parse_args()
    supported_type = ['all', 'index', 'schema', 'config', 'space']
    if options.type not in supported_type:
        print('Given wrong type: %s, must be one of %s' % (options.type, supported_type))
        exit(1)

    print('Nebula graph address {}:{}'.format(options.ip, options.port))
    generator = Generator()
    generator.start()
    # generator.print()
    generator.save_to_file()

