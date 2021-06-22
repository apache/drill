---
title: "IP Networking functions"
slug: "IP Networking functions"
parent: "SQL Functions"
---

**Introduced in release:** 1.12.

A set of functions for common computations on IP network addresses.

| Function                  | Output  | Description                                               |
| ------------------------- | ------- | --------------------------------------------------------- |
| inet_aton( ip )           | INT     | Converts an IPv4 address into an integer                  |
| inet_ntoa( int )          | VARCHAR | Converts an integer IP into dotted decimal notation       |
| in_network( ip, cidr )    | BOOLEAN | Returns true if the IP address is in the given CIDR block |
| address_count( cidr )     | INT     | Returns the number of IPs in a given CIDR block           |
| broadcast_address( cidr ) | VARCHAR | Returns the broadcast address for a given CIDR block      |
| netmask( cidr )           | VARCHAR | Returns the netmask for a given CIDR block                |
| low_address( cidr )       | VARCHAR | Returns the first address in a given CIDR block           |
| high_address( cidr )      | VARCHAR | Returns the last address in a given CIDR block            |
| url_encode( url )         | VARCHAR | Returns a URL encoded string                              |
| url_decode( url )         | VARCHAR | Decodes a URL encoded string                              |
| is_valid_IP( ip )         | BOOLEAN | Returns true if the IP is a valid IP address              |
| is_private_ip( ip )       | BOOLEAN | Returns true if the IP is a private IPv4 address          |
| is_valid_IPv4( ip )       | BOOLEAN | Returns true if the IP is a valid IPv4 address            |
| is_valid_IPv6( ip )       | BOOLEAN | Returns true if the IP is a valid IPv6 address            |
