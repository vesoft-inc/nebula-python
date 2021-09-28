#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import logging


from nebula2.common.ttypes import ErrorCode
from nebula2.Exception import (
    AuthFailedException,
    IOErrorException,
    NotValidConnectionException,
    InValidHostname,
)

from nebula2.data.ResultSet import ResultSet

from nebula2.gclient.net.AuthResult import AuthResult
from nebula2.gclient.net.Session import Session
from nebula2.gclient.net.Connection import Connection
from nebula2.gclient.net.ConnectionPool import ConnectionPool

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)-8s [%(filename)s:%(lineno)d]:%(message)s')
