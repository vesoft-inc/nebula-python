#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


class OutOfRangeException(Exception):
    def __init__(self):
        Exception.__init__(self)
        self.message = 'list index out of range'


class InvalidKeyException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = "KeyError: `{}'".format(message)


class InvalidValueTypeException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = "Invalid value type: `{}'".format(message)


class AuthFailedException(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = 'Auth failed: {}'.format(message)


class TagNotFoundException(Exception):
    def __init__(self, name):
        Exception.__init__(self)
        self.message = 'Tag:{} not found'.format(name)


class EdgeNotFoundException(Exception):
    def __init__(self, name):
        Exception.__init__(self)
        self.message = 'Edge:{} not found'.format(name)


class SpaceNotFoundException(Exception):
    def __init__(self, name):
        Exception.__init__(self, name)
        self.message = 'Space:{} not found'.format(name)


class PartNotFoundException(Exception):
    def __init__(self, part):
        Exception.__init__(self)
        self.message = 'Partition:{} not found'.format(part)


class NotValidConnectionException(Exception):
    def __init__(self):
        Exception.__init__(self)
        self.message = 'No extra connection'


class InValidHostname(Exception):
    def __init__(self, message):
        Exception.__init__(self, message)
        self.message = 'Invalid hostname: {}'.format(message)


class IOErrorException(Exception):
    E_UNKNOWN = 0
    E_ALL_BROKEN = 1
    E_CONNECT_BROKEN = 2
    E_TIMEOUT = 3
    E_NOT_OPEN = 4

    def __init__(self, code=E_UNKNOWN, message=None):
        Exception.__init__(self, message)
        self.type = code
        self.message = message


class ClientServerIncompatibleException(Exception):
    def __init__(self, message):
        Exception.__init__(
            self,
            f'Current client is not compatible with the remote server, please check the version: {message}',
        )
