# signout--coding:utf-8--

# Copyright (c) 2019 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.


class AuthException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


class ExecutionException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = message


class SimpleResponse:
    """
    Attributes:
         - error_code
         - error_msg
    """
    def __init__(self, code, msg):
        self.error_code = code
        self.error_msg = msg
