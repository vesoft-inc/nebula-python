# nebula-python

This repository provides Nebula client API in Python.

## The directory structure

```text
|--nebula-python
    |
    |-- nebula                        // client code
    |   |-- Client.py                 // interfaces controlling nebula
    |   |-- ConnectionPool.py         // the connection pool that manages all the connections, users can specify the connection numbers and timeout when creating it
    |
    |-- common                        // the common data types
    |
    |-- graph                         // date types and client interfaces interact with graphd
    |
    |-- meta                          // date types and client interfaces interact with metad
    |
    |-- raftex                        // date types and client interfaces interact with raftex
    |
    |-- storage                       // date types and client interfaces interact with storaged
    |
    |__ thrift                        // the socket implementation code
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

### Option One

- Cloning from GitHub

```bash
git clone git@github.com:laura-ding/nebula-python.git
cd nebula-python
```

- Install

```python
python setup.py install
```

### Option Two

- Using pip

```python
pip install nebula-python
```

## How to use nebula-python in your code

There are three major modules:

- ConnectionPool.py
- Client.py
- ttypes.py

Please refer to the [sample code](examples/ClientExample.py) on detail usage.

- Steps to create a client
  - Step1: create a connection pool
  - Step2: create a client through the connection pool
  - Step3: authenticate
  - Step4: execute/execute_query
  - Step5: return the client to pool and close pool

## Install by pip

- django-import-export
- future
- six
- httplib2
- futures   # python2.x is needed
