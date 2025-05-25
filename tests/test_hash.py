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
    (b"abcdefgh", 8664279048047335611),  # length-8 bytes cases
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


def test_seed_variation():
    """Different seed values should produce different hashes."""
    data = b"seed_test"
    hash1 = murmur_hash(data, seed=0)
    hash2 = murmur_hash(data, seed=1)
    assert hash1 != hash2


def test_idempotent():
    """Repeated calls with same input must yield the same result."""
    data = b"consistent"
    assert murmur_hash(data) == murmur_hash(data)


def test_large_input_performance():
    """Large inputs should be processed without error and return an int."""
    data = b"x" * 10_000
    result = murmur_hash(data)
    assert isinstance(result, int)
