# nebula-python

[![star this repo](http://githubbadges.com/star.svg?user=vesoft-inc&repo=nebula-python&style=default)](https://github.com/vesoft-inc/nebula-python)
[![fork this repo](http://githubbadges.com/fork.svg?user=vesoft-inc&repo=nebula-python&style=default)](https://github.com/vesoft-inc/nebula-python/fork)

This repository provides Nebula client API in Python.

## The directory structure

```text
|--nebula-python
    |
    |-- nebula
    |   |-- Common.py                 // define exception and simple response
    |   |
    |   |-- Client.py                 // interfaces controlling nebula
    |   |
    |   |-- ConnectionPool.py         // the connection pool that manages all the connections, users can specify the connection numbers and timeout when creating it
    |   |
    |   |-- common                    // the common data types
    |   |
    |   |-- graph                     // data types and client interfaces to interact with graphd
    |   |
    |   |-- meta                      // data types and client interfaces to interact with metad
    |   |
    |   |-- storage                   // data types and client interfaces to interact with storaged
    |   |
    |   |__ thrift                    // the socket implementation code
    |
    |-- examples
    |   |__ ClientExample.py          // the example code
    |
    |-- tests
    |   |__ test_client.py            // the test code
    |
    |-- setup.py                      // used to install or package
    |
    |-- README.md                     // the introduce of nebula-python
    |
    |__ LICENSES                      // license file
```

## How to get nebula-python

### Option One: cloning from GitHub

- Cloning

```bash
git clone https://github.com/vesoft-inc/nebula-python.git
cd nebula-python
```

- Install

python2

```python
sudo python setup.py install
```

python3

```python
sudo python3 setup.py install
```

Note that when installing via python3, the error message msg `extras_require = {':python_version == "2.7"':['futures']}` will appear. There is no such package under python3, just ignore this error.

When your environment cannot access `pypi`, you need to manually install the following packages.

- django-import-export
- future
- six
- httplib2
- futures   # python2.x is needed

### Option Two: using pip

```python
sudo pip install nebula-python
```

## How to use nebula-python in your code

There are three major modules:

- nebula/ConnectionPool.py
- nebula/Client.py
- nebula/Common.py
- nebula/graph/ttypes.py

Please refer to the [sample code](examples/ClientExample.py) on detail usage.
If you want to run the sample code, please install `prettytable` and `networkx` via pip.

### Steps to create a client
  - Step1: create a connection pool
    - The default connection number of the connection pool is **two**
    - The default timeout of connection is **1000ms**, **0** means no timeout
    - When the created clients exceed the number of connections in the connection pool, the clients that exceed the number of connections will enter the wait state, waiting for the previous clients to release before continuing
  - Step2:  create a client through the connection pool, and call `GraphClient.set_space` to set the space which will be used by reconnecting when the connection is broken
  - Step3: authenticate
  - Step4: execute/execute\_query
  - Step5: return the client to pool and close pool

### Quick Example
```python
from nebula.graph import ttypes
from nebula.ConnectionPool import ConnectionPool
from nebula.Client import GraphClient

connection_pool = ConnectionPool('127.0.0.1', 3699)
client = GraphClient(connection_pool)
auth_resp = client.authenticate('user', 'password')
query_resp = client.execute_query('SHOW SPACES')
client.sign_out()
connection_pool.close()
```

## How to choose nebula-python

| Nebula-Python Version | NebulaGraph Version |
|---|---|
| 1.0.0rc1  | 1.0.0-rc1 |
| 1.0.0-rc2-1 | 1.0.0-rc2 / 1.0.0-rc3 |
| 1.0.0rc4 | 1.0.0-rc4 |
| 1.0.0.post0 | \>= 1.0.0 |
| 1.1.0 | \>= 1.1.0 |
| 1.1.1 | \>= 1.1.0 |
