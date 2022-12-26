## How to update the generate files when the thrift file had change in repo `https://github.com/vesoft-inc/nebula`

- download the thrift binary from oss, the binary file was build under Fedora30

```
wget https://oss-cdn.nebula-graph.com.cn/fbthrift_bin/thrift1
```
- use the binary file `thrift1` and the thrift
files from `https://github.com/vesoft-inc/nebula/tree/master/src/interface` to generate the interface files

```
./thrift1 --strict --allow-neg-enum-vals --gen "py" -o . common.thrift
./thrift1 --strict --allow-neg-enum-vals --gen "py" -o . graph.thrift
./thrift1 --strict --allow-neg-enum-vals --gen "py" -o . meta.thrift
./thrift1 --strict --allow-neg-enum-vals --gen "py" -o . storage.thrift
```