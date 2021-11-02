# nebula-python

This directory holds the Python API for Nebula Graph. It is used to connect with Nebula Graph 2.0.

## Before you start

Before you start, please read this section to choose the right branch for you. In branch v1.0, the API works only for Nebula Graph 1.0. In the master branch, the API works only for Nebula Graph 2.0.

## The directory structure

```text
|--nebula-python
    |
    |-- nebula2                               // client code
    |   |-- fbthrift                          // the fbthrift lib code
    |   |-- common           
    |   |-- data           
    |   |-- graph           
    |   |-- meta           
    |   |-- net                               // the net code for graph client
    |   |-- storage           
    |   |-- Config.py                         // the pool config
    |   |__ Exception.py                      // the define exception
    |           
    |-- examples
    |   |-- GraphClientMultiThreadExample.py  // the multi thread example
    |   |-- GraphClientSimpleExample.py       // the simple example
    |   |__ ScanVertexEdgeExample.py                   
    |
    |-- tests                                 // the test code
    |                      
    |-- setup.py                              // used to install or package
    |                      
    |__ README.md                             // the introduction of nebula2-python

```

## How to get nebula2-python

### Option one: clone from GitHub

- Clone from GitHub

```bash
git clone https://github.com/vesoft-inc/nebula-python.git
cd nebula-python
```

- Install

```python
sudo python3 setup.py install
```

When your environment cannot access `pypi`, you need to install the following packages manually.

```
pip install -r requirements.txt
```
dev version
```
pip install -r requirements-dev.txt
```

### Option two: using pip

```python
pip install nebula2-python==$version
```

## Quick example to use graph-client to connect graphd

```python
from nebula2.gclient.net import ConnectionPool
from nebula2.Config import Config

# define a config
config = Config()
config.max_connection_pool_size = 10
# init connection pool
connection_pool = ConnectionPool()
# if the given servers are ok, return true, else return false
ok = connection_pool.init([('127.0.0.1', 9669)], config)

# option 1 control the connection release yourself
# get session from the pool
session = connection_pool.get_session('root', 'nebula')

# select space
session.execute('USE nba')

# show tags
result = session.execute('SHOW TAGS')
print(result)

# release session
session.release()

# option 2 with session_context, session will be released automatically
with connection_pool.session_context('root', 'nebula') as session:
    session.execute('USE nba')
    result = session.execute('SHOW TAGS')
    print(result)

# close the pool
connection_pool.close()
```

## Quick example to use storage-client to scan vertex and edge

You should make sure the scan client can connect to the address of storage which see from `SHOW HOSTS` 

```python
from nebula2.mclient import MetaCache, HostAddr
from nebula2.sclient.GraphStorageClient import GraphStorageClient

# the metad servers's address
meta_cache = MetaCache([('172.28.1.1', 9559),
                        ('172.28.1.2', 9559),
                        ('172.28.1.3', 9559)],
                       50000)

# option 1 metad usually discover the storage address automatically
graph_storage_client = GraphStorageClient(meta_cache)

# option 2 manually specify the storage address
storage_addrs = [HostAddr(host='172.28.1.4', port=9779),
                 HostAddr(host='172.28.1.5', port=9779),
                 HostAddr(host='172.28.1.6', port=9779)]
graph_storage_client = GraphStorageClient(meta_cache, storage_addrs)

resp = graph_storage_client.scan_vertex(
        space_name='ScanSpace',
        tag_name='person')
while resp.has_next():
    result = resp.next()
    for vertex_data in result:
        print(vertex_data)
        
resp = graph_storage_client.scan_edge(
    space_name='ScanSpace',
    edge_name='friend')
while resp.has_next():
    result = resp.next()
    for edge_data in result:
        print(edge_data)
```

## How to choose nebula-python

| Nebula2-Python Version | NebulaGraph Version |
|---|---|
| 2.0.0b1  | 2.0.0beta |
| 2.0.0rc1  | 2.0.0-rc1 |
| 2.0.0  | 2.0.0/2.0.1 |
| 2.5.0  | 2.5.0 |
| 2.6.0  | 2.6.0 |
