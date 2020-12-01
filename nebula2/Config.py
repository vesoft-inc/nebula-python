#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


class Config(object):
    # the min connection always in pool
    min_connection_pool_size = 0

    # the max connection in pool
    max_connection_pool_size = 10

    # connection or execute timeout, unit ms, 0 means no timeout
    timeout = 0

    # unit s, 0 means will never close the idle connection
    idle_time = 0
