## Overview

A core component of any network security program is analyzing raw data that is coming over the wire. This raw network data is captured in a format called Packet Capture (PCAP) or PCAP Next Generation (PCAP­NG) and can be challenging to analyze because it is a binary format. One of the most common tools for analyzing PCAP data is called Wireshark. Even though Wireshark is a capable tool, it is limited in that it can only analyze data that fits in your system’s memory.

<i> -- taken from "Learning Apache Drill" Book.</i>

Drill can query a PCAP or PCAP­NG file and retrieve fields including the following:

 - Protocol type (TCP/UDP)

 - Source/destination IP address Source/destination port

 - Source/destination MAC address

 - Date and time of packet creation

 - Packet length

 - TCP session number and flags

 - The packet data in binary form
 
Querying PCAP or PCAP­NG requires no additional configuration settings, so out of the box, Drill installation can query them both.

## Attributes

The following table lists configuration attributes:

Attribute|Default Value|Description
---------|-------------|-----------
stat|false|return the statistics data about the each pcapng file if true
sessionizeTCPStreams|false|return one row per TCP session instead of one row per packet, for both PCAP and PCAP-NG. A session is written when it closes (FIN or RST). Sessions still open at the end of the file, such as ones that outlived the capture, are written last with `session_closed` false
exposeCredentials|false|include cleartext passwords found by protocol decoders in `parsed_data`; usernames and a `password_present` flag are always included

## PCAP-NG columns

PCAP-NG files are streamed one block at a time, so file size is not limited by memory.

In addition to the packet columns above, packet queries return the following metadata.
The time of each packet is in `packet_timestamp`, the same column name as in PCAP files. (It was named
`timestamp` in PCAP-NG files before; that name is a reserved word that had to be quoted in every query.)
Timestamps honor each interface's `if_tsresol` and `if_tsoffset`.

Column|Description
------|-----------
captured_length|Bytes of the packet stored in the file (`packet_length` is the original length on the wire)
interface_id|Index of the capturing interface within its section
interface_name|`if_name` of the capturing interface
link_type|[LINKTYPE_ value](https://www.tcpdump.org/linktypes.html) of the capturing interface
comment|Packet comments (`opt_comment`), newline separated
direction|`inbound` or `outbound` (`epb_flags`)
reception_type|`unicast`, `multicast`, `broadcast` or `promiscuous` (`epb_flags`)
fcs_length|Frame check sequence length in octets (`epb_flags`)
drop_count|Packets lost since the preceding packet (`epb_dropcount`)
packet_hash|`epb_hash` as `algorithm:hex`, such as `md5:9e107d9d...`

IP, port and TCP columns are decoded for these link types: Ethernet (1), raw IP (101, 228, 229),
BSD loopback (0, 108), PPP (9), Linux cooked capture v1 and v2 (113, 276). On other link types they are null,
and the MAC address columns are only set for Ethernet.

With `stat` set to true, options a block does not have are null. The `comment` column holds the section, interface, name resolution or
statistics block comment.

## Protocol decoding

Packets and TCP sessions whose application protocol is recognized are decoded into three columns, in both
PCAP and PCAP-NG files:

Column|Description
------|-----------
parsed_protocol|Name of the decoder that handled the row, such as `dns` or `http`; null if none did
parsed_data|One map per decoder, named after its protocol; only the one named by `parsed_protocol` is filled
decode_error|Why the row could not be fully decoded; null when there was no problem

```sql
-- DNS questions and answers
SELECT t.parsed_data.dns.questions[0].name AS query, t.parsed_data.dns.answers
FROM dfs.`capture.pcapng` t
WHERE parsed_protocol = 'dns';

-- HTTP requests, one row per connection
SELECT src_ip, t.parsed_data.http.exchanges[0].host AS host, t.parsed_data.http.exchanges[0].uri AS uri
FROM table(dfs.`capture.pcap` (type => 'pcap', sessionizeTCPStreams => true)) t
WHERE parsed_protocol = 'http';

-- What could not be decoded
SELECT decode_error, count(*) FROM dfs.`capture.pcapng`
WHERE decode_error IS NOT NULL GROUP BY decode_error;
```

Built-in decoders:

Protocol|Rows|Matches
--------|----|-------
dns|packets|DNS, mDNS and LLMNR on ports 53, 5353 and 5355
dns|sessions|DNS over TCP port 53, such as zone transfers
http|packets and sessions|HTTP/1.x on ports 80, 591, 3128, 8000, 8008, 8080 and 8888
icmp|packets|ICMP and ICMPv6
arp|packets|ARP
ntp|packets|UDP 123
syslog|packets|UDP 514, RFC 5424 and BSD formats
ssdp|packets|UDP 1900
sip|packets|UDP and TCP 5060
dhcp|packets|UDP 67 and 68
dhcpv6|packets|UDP 546 and 547
tftp|packets|UDP 69
netbios_ns|packets|UDP 137
radius|packets|UDP 1812, 1813, 1645 and 1646
snmp|packets|UDP 161 and 162, versions 1, 2c and 3
tls|packets|ClientHello and ServerHello (SNI, ALPN, JA3, JA3S) on TLS ports such as 443, 993 and 8443
tls|sessions|Full handshake on the same ports: negotiated version and cipher, certificates
stun|packets|UDP 3478 and 19302
ftp|sessions|FTP control channel on port 21
ssh|sessions|SSH identification and key exchange (HASSH) on ports 22 and 2222
smtp|sessions|SMTP on ports 25, 587 and 2525: credentials, envelope, message headers
pop3|sessions|POP3 on port 110: credentials, retrieved message headers
imap|sessions|IMAP on port 143: credentials, mailboxes, fetched message headers

A decoder only handles traffic that parses as its protocol: other traffic on the same port is left undecoded.
Decoding runs only when `parsed_protocol` or `parsed_data` is queried.

Errors never stop a file from being read. Malformed packets keep the fields that could be read and explain
the problem in `decode_error`. Damage that makes the rest of a file unreadable, or a file that is not a
capture at all, produces one row with only `decode_error` set; exclude such rows with
`WHERE decode_error IS NULL`.

Fields of each decoder, and how to write a decoder of your own, are described in
`docs/dev/PcapProtocolDecoders.md`.
