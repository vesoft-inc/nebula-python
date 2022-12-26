#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.


from nebula3.common.ttypes import Value

Value.__hash__ = lambda self: self.value.__hash__()
