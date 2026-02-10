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

"""Address parsing utilities for NebulaGraph clients.

This module provides centralized address parsing functionality supporting
IPv4, IPv6, and hostname formats. It's designed to be used across all
client and pool implementations without creating circular dependencies.
"""

from typing import List, Union

from nebulagraph_python.data import HostAddress


def parse_address(address: str) -> HostAddress:
    """Parse a single address string (IPv4, IPv6, or hostname) into HostAddress.
    
    Supports:
    - IPv4: "127.0.0.1:9669"
    - IPv6 with brackets: "[2001:db8::1]:9669"
    - IPv6 full: "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:9669"
    - IPv6 compressed: "[fe80::1]:9669"
    - IPv6 localhost: "[::1]:9669"
    - IPv6 with zone index: "[fe80::1%eth0]:9669"
    - Hostname: "localhost:9669"
    
    Args:
        address: Address string to parse
        
    Returns:
        HostAddress object
        
    Raises:
        ValueError: If address format is invalid
        
    Examples:
        >>> parse_address("127.0.0.1:9669")
        HostAddress(host='127.0.0.1', port=9669)
        
        >>> parse_address("[2001:db8::1]:9669")
        HostAddress(host='2001:db8::1', port=9669)
        
        >>> parse_address("[::1]:9669")
        HostAddress(host='::1', port=9669)
    """
    address = address.strip()
    
    # IPv6 address with brackets: [2001:db8::1]:9669
    if address.startswith("["):
        if "]" not in address:
            raise ValueError(f"Invalid IPv6 address format: {address}")
        # Find the closing bracket
        bracket_end = address.index("]")
        host = address[1:bracket_end]  # Extract address between brackets
        
        # Check if port is specified after bracket
        if bracket_end + 1 < len(address):
            if address[bracket_end + 1] != ":":
                raise ValueError(f"Invalid IPv6 address format, expected ':' after ']': {address}")
            port_str = address[bracket_end + 2:]
            if not port_str:
                raise ValueError(f"Port number missing after ':': {address}")
            port = int(port_str)
        else:
            raise ValueError(f"Port number missing for IPv6 address: {address}")
            
        return HostAddress(host, port)
    
    # IPv4 or hostname: 127.0.0.1:9669 or localhost:9669
    # Use rsplit to handle case where host might have only one colon (IPv4)
    # But we need to be careful - if there are multiple colons (IPv6 without brackets),
    # we should take the last colon as port separator
    if ":" in address:
        parts = address.rsplit(":", 1)
        host = parts[0]
        port_str = parts[1]
        
        # Validate port is a number
        try:
            port = int(port_str)
        except ValueError:
            raise ValueError(f"Invalid port number: {port_str}")
            
        return HostAddress(host, port)
    
    raise ValueError(f"Invalid address format: {address}")


def parse_hosts(hosts: Union[str, List[str], List[HostAddress]]) -> List[HostAddress]:
    """Convert various host formats to list of HostAddress objects.
    
    This is a convenience function that accepts multiple input formats:
    - A single string with comma-separated addresses
    - A list of address strings
    - A list of HostAddress objects (returned as-is)
    - A mixed list of strings and HostAddress objects
    
    Args:
        hosts: Host specification in one of the supported formats
        
    Returns:
        List of HostAddress objects
        
    Raises:
        ValueError: If any address format is invalid
        
    Examples:
        >>> parse_hosts("127.0.0.1:9669")
        [HostAddress(host='127.0.0.1', port=9669)]
        
        >>> parse_hosts("127.0.0.1:9669,[2001:db8::1]:9669")
        [HostAddress(host='127.0.0.1', port=9669), HostAddress(host='2001:db8::1', port=9669)]
        
        >>> parse_hosts(["127.0.0.1:9669", "[::1]:9669"])
        [HostAddress(host='127.0.0.1', port=9669), HostAddress(host='::1', port=9669)]
    """
    if isinstance(hosts, str):
        hosts = hosts.split(",")

    addresses = []
    for host in hosts:
        if isinstance(host, HostAddress):
            addresses.append(host)
        else:
            addresses.append(parse_address(host))
    return addresses


# Export public API
__all__ = ["parse_address", "parse_hosts"]