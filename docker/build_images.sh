#!/bin/bash
#
# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

docker build -t nebula-python2:v1.0 -f ./Dockerfile.2 .
if [[ $? -ne 0 ]]; then
    echo "build image nebula-python2:v1.0 failed"
fi

docker build -t nebula-python3:v1.0 -f ./Dockerfile.3 .
if [[ $? -ne 0 ]]; then
    echo "build image nebula-python3:v1.0 failed"
fi

docker push nebula-python2:v1.0
docker push nebula-python3:v1.0
