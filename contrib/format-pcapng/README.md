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

## PCAP-NG columns

PCAP-NG files are streamed one block at a time, so file size is not limited by memory.

In addition to the packet columns above, packet queries return the following metadata.
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
