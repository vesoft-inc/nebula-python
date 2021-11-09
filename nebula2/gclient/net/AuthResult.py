# --coding:utf-8--
#
# Copyright (c) 2021 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


class AuthResult(object):
    def __init__(self, session_id, timezone_offset, timezone_name):
        self._session_id = session_id
        self._timezone_offset = timezone_offset
        self._timezone_name = timezone_name

    def get_session_id(self):
        return self._session_id

    def get_timezone_offset(self):
        return self._timezone_offset

    def get_timezone_name(self):
        return self._timezone_name
