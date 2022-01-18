## v2.5.0(2021-08-17)
Compatible with the v2.5.0 version of nebula-graph

- feature
	- add TimeWrapper/DateTimeWrapper type to get timezone obtained from the server to calculate the local time
	- Session supports reconnecting to different graph services
- bugfix
	- fix the interface values of Relationship and modify the interface propertys to properties https://github.com/vesoft-inc/nebula-python/pull/113
	- fix get offline host info from list_hosts https://github.com/vesoft-inc/nebula-python/pull/104
	- fix fbthrift timeout bug https://github.com/vesoft-inc/nebula-python/pull/126
- incompatible
	- the ErrorCode define is changed, all ErrorCode is defined in nebula3.common.ttypes.ErrorCode
	
## v2.0.0(2021-03-23)
Compatible with the v2.0.0 version of nebula-graph

- New features
	- Support to use with nebula-graph2.0
	
## v2.0.0rc1(2021-01-06)
Compatible with the v2.0.0-RC1 version of nebula-graph

- New features
	- Support to scan vertexes and edges
	- Support more data type function

## v2.0.0-1(2020-11-30)
Compatible with the v2.0.0-beta version of nebula-graph

- New features
	- Support to use with nebula-graph2.0
