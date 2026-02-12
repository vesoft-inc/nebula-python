# Copyright 2025 vesoft-inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

from nebulagraph_python.client.address_utils import parse_address, parse_hosts
from nebulagraph_python.data import HostAddress


class TestIPv6AddressParsing:
    """Test IPv6 address parsing functionality"""

    def test_parse_ipv4_address(self):
        """Test parsing IPv4 addresses"""
        result = parse_address("127.0.0.1:9669")
        assert result.host == "127.0.0.1"
        assert result.port == 9669

    def test_parse_hostname_with_port(self):
        """Test parsing hostname with port"""
        result = parse_address("localhost:9669")
        assert result.host == "localhost"
        assert result.port == 9669

    def test_parse_ipv6_with_brackets(self):
        """Test parsing IPv6 address with brackets"""
        result = parse_address("[2001:db8::1]:9669")
        assert result.host == "2001:db8::1"
        assert result.port == 9669

    def test_parse_ipv6_full_with_brackets(self):
        """Test parsing full IPv6 address with brackets"""
        result = parse_address("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:9669")
        assert result.host == "2001:0db8:85a3:0000:0000:8a2e:0370:7334"
        assert result.port == 9669

    def test_parse_ipv6_localhost_with_brackets(self):
        """Test parsing IPv6 localhost with brackets"""
        result = parse_address("[::1]:9669")
        assert result.host == "::1"
        assert result.port == 9669

    def test_parse_ipv6_all_zeros_with_brackets(self):
        """Test parsing IPv6 all zeros with brackets"""
        result = parse_address("[::]:9669")
        assert result.host == "::"
        assert result.port == 9669

    def test_parse_ipv6_compressed_with_brackets(self):
        """Test parsing compressed IPv6 address with brackets"""
        result = parse_address("[fe80::1]:9669")
        assert result.host == "fe80::1"
        assert result.port == 9669

    def test_parse_ipv6_mixed_compressed_with_brackets(self):
        """Test parsing mixed compressed IPv6 address with brackets"""
        result = parse_address("[2001:db8::1234:5678]:9669")
        assert result.host == "2001:db8::1234:5678"
        assert result.port == 9669

    def test_parse_ipv6_missing_closing_bracket(self):
        """Test that missing closing bracket raises ValueError"""
        with pytest.raises(ValueError, match="Invalid IPv6 address format"):
            parse_address("[2001:db8::1")

    def test_parse_ipv6_missing_port(self):
        """Test that missing port after bracket raises ValueError"""
        with pytest.raises(ValueError, match="Port number missing"):
            parse_address("[2001:db8::1]")

    def test_parse_invalid_address_format(self):
        """Test that invalid address format raises ValueError"""
        with pytest.raises(ValueError, match="Invalid address format"):
            parse_address("invalid")

    def test_parse_invalid_port(self):
        """Test that invalid port raises ValueError"""
        with pytest.raises(ValueError, match="Invalid port number"):
            parse_address("127.0.0.1:abc")

    def test_parse_ipv6_wrong_separator_after_bracket(self):
        """Test that wrong separator after bracket raises ValueError"""
        with pytest.raises(ValueError, match="expected ':' after"):
            parse_address("[2001:db8::1]-9669")

    def test_parse_hosts_string(self):
        """Test parsing multiple hosts from string"""
        result = parse_hosts("127.0.0.1:9669,[2001:db8::1]:9669,localhost:9770")
        assert len(result) == 3
        assert result[0].host == "127.0.0.1"
        assert result[0].port == 9669
        assert result[1].host == "2001:db8::1"
        assert result[1].port == 9669
        assert result[2].host == "localhost"
        assert result[2].port == 9770

    def test_parse_hosts_list(self):
        """Test parsing hosts from list"""
        result = parse_hosts(["127.0.0.1:9669", "[::1]:9669"])
        assert len(result) == 2
        assert result[0].host == "127.0.0.1"
        assert result[0].port == 9669
        assert result[1].host == "::1"
        assert result[1].port == 9669

    def test_parse_hosts_with_hostaddress_objects(self):
        """Test parsing hosts with HostAddress objects"""
        host_addr = HostAddress("127.0.0.1", 9669)
        result = parse_hosts([host_addr, "[2001:db8::1]:9669"])
        assert len(result) == 2
        assert result[0].host == "127.0.0.1"
        assert result[0].port == 9669
        assert result[1].host == "2001:db8::1"
        assert result[1].port == 9669

    def test_parse_address_with_whitespace(self):
        """Test that whitespace is stripped from addresses"""
        result = parse_address("  127.0.0.1:9669  ")
        assert result.host == "127.0.0.1"
        assert result.port == 9669

    def test_parse_hosts_with_whitespace(self):
        """Test that whitespace is stripped from multiple hosts"""
        result = parse_hosts(" 127.0.0.1:9669 , [2001:db8::1]:9669 , localhost:9770 ")
        assert len(result) == 3
        assert result[0].host == "127.0.0.1"
        assert result[1].host == "2001:db8::1"
        assert result[2].host == "localhost"

    def test_parse_ipv6_with_zone_index(self):
        """Test parsing IPv6 address with zone index"""
        result = parse_address("[fe80::1%eth0]:9669")
        assert result.host == "fe80::1%eth0"
        assert result.port == 9669