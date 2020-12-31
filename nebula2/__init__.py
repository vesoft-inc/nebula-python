#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

import sys
import os
import nebula2

nebula_path = os.path.dirname(nebula2.__file__)
sys.path.insert(0, nebula_path)

from nebula2.common.ttypes import Value
nebula2.common.ttypes.Value.__hash__ = lambda self: self.value.__hash__()


