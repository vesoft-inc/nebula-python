# nebula-python

Nebula client API in Python

## the directory structure

```text
|--nebula-python
    |
    |-- nebula                        // client code
    |   |-- Client.py                 // the interfaces to operate nebula 
    |   |-- ConnectionPool.py         // manger all connection, user can change the connection nums and timeout
    |
    |-- common                        // the common types
    |
    |-- graph                         // the types with graphd, and the client with graphd
    |
    |-- meta                          // the types with metad, and the client with metad
    |
    |-- raftex                        // the types with raftex, and the client with raftex
    |
    |-- storage                       // the types with stroged, and the client with stroged
    |
    |__ thrift                        // the thrift code, about the socket
    |   
    |-- examples               
    |   |__ ClientExample.py          // the example
    |
    |-- tests
    |   |__ test_client.py            // the test file
    |
    |-- setup.py                      // use to install or package
    |
    |-- README.md                     // the introduce of nebula-python 
    |
    |__ LICENSES                      // license file
```

## how to get the nebula-python

Method 1
- get the nebula-python src from github

```bash
git clone git@github.com:laura-ding/nebula-python.git
cd nebula-python
```
- install

```python
python setup.py install
```

Method 2 
- get from pypi

```python
pip install nebula-python
```

## how to use nebula-python in your code
There are three major modules: Client.py, ConnectionPool.py, ttypes.py.

- ConnectionPool.py
- Client.py
- ttypes.py

You'll use them to do things. Please see the sample code [example](https://github.com/vesoft-inc/nebula-python/tree/master/examples/ClientExample.py)

- Steps to create a client
    - Step1: create a connection pool
    - Step2: create a client through the connection pool
    - Step3: authenticate
    - Step4: execute/execute_query
    - Step5: return the client to pool and close pool

## install by pip
- django-import-export
- future
- six
- httplib2
- gevent
- futures   # python2.x need
