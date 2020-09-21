## v1.1.0(2020-09-21)
- Compatible with the 1.1.0 version of nebula-graph

- New features
    - Support using storage client to scan vertices and scan edges
- Changes
    - The `ExecutionResponse` add optional `warning_msg`
    - Delete the debug log

## v1.0.0(2020-06-08)
- Compatible with the 1.0.0 version of nebula-graph

- New features
    - reconnect
        - Users need to call GraphClient's set_space() to set the space which is used after the session reconnects.
    - thread-safety
        - The **GraphClient** is thread-safety.

## v1.0.0rc4(2020-03-23)
Compatible with the 1.0.0-rc4 version of nebula-graph

- New features
	- None
- Changes
	- Delete the parameter `is_async` in `ConnectionPool`
	- Move `AuthException`, `ExecutionException` and `SimpleResponse` from `nebula/Client.py` to `nebula/Common.py`.  You need to add `from nebula.Common import *` in your project.

## v1.0.0-rc2-1(2019-12-05)
Compatible with the 1.0.0-rc2 and 1.0.0-rc3 version of nebula-graph

- New features
	- None
- Changes
	- Update thrift

## v1.0.0rc1(2019-11-27)
Compatible with the 1.0.0-rc1 version of nebula-graph

- New features
	- Support to use with nebula-graph
