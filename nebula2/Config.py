#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


import ssl


class Config(object):
    # the min connection always in pool
    min_connection_pool_size = 0

    # the max connection in pool
    max_connection_pool_size = 10

    # connection or execute timeout, unit ms, 0 means no timeout
    timeout = 0

    # 0 means will never close the idle connection, unit ms,
    idle_time = 0

    # the interval to check idle time connection, unit second, -1 means no check
    interval_check = -1


class SSL_config(object):
    """configs used to Initialize a TSSLSocket.
    @ ssl_version(int)              protocol version. see ssl module. If none is
                                    specified, we will default to the most
                                    reasonably secure and compatible configuration
                                    if possible.
                                    For Python versions >= 2.7.9, we will default
                                    to at least TLS 1.1.
                                    For Python versions < 2.7.9, we can only
                                    default to TLS 1.0, which is the best that
                                    Python guarantees to offers at this version.
                                    If you specify ssl.PROTOCOL_SSLv23, and
                                    the OpenSSL linked with Python is new enough,
                                    it is possible for a TLS 1.2 connection be
                                    established; however, there is no way in
                                    < Python 2.7.9 to explicitly disable SSLv2
                                    and SSLv3. For that reason, we default to
                                    TLS 1.0.

    @ cert_reqs(int)                whether to verify peer certificate. see ssl
                                    module.

    @ ca_certs(str)                 filename containing trusted root certs.

    @ verify_name                   if False, no peer name validation is performed
                                    if True, verify subject name of peer vs 'host'
                                    if a str, verify subject name of peer vs given
                                    str

    @ keyfile                       filename containing the client's private key

    @ certfile                      filename containing the client's cert and
                                    optionally the private key

    @ allow_weak_ssl_versions(bool) By default, we try to disable older
                                            protocol versions. Only set this
                                            if you know what you are doing.
    """

    unix_socket = None
    ssl_version = None
    cert_reqs = ssl.CERT_NONE
    ca_certs = None
    verify_name = False
    keyfile = None
    certfile = None
    allow_weak_ssl_versions = False
