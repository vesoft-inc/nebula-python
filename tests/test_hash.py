#!/usr/bin/env python
# --coding:utf-8--

# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import pytest
from nebula3.utils.hash import hash as murmur_hash

TEST_VECTORS = [
    (b"", 6142509188972423790),
    (b"a", 4993892634952068459),
    (b"abcdefgh", 8664279048047335611),  # length-8 cases
    (b"abcdefghi", -5409788147785758033),
    ("to_be_hashed", -1098333533029391540),
    ("中文", -8591787916246384322),
]

@pytest.mark.parametrize("data, expected", TEST_VECTORS)
def test_known_vectors(data, expected):
    assert murmur_hash(data) == expected


def test_str_bytes_equiv():
    """
    Ensure str and bytes inputs produce the same hash.
    """
    s = "pytest"
    assert murmur_hash(s) == murmur_hash(s.encode("utf-8"))


def test_type_error():
    """
    TypeError
    """
    with pytest.raises(TypeError):
        murmur_hash(12345)
