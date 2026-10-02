# PCAP Protocol Decoders

Design for decoding application protocols (DNS, HTTP, SMTP, and so on) in the PCAP and PCAP-NG format
plugin (`contrib/format-pcapng`), and the guide for writing new decoders. Tracked under
[DRILL-8556](https://issues.apache.org/jira/browse/DRILL-8556).

## Goals

- Parse the payload of packets and TCP sessions whose protocol is known, and return the fields as Drill
  data that can be queried directly.
- Make it easy to add decoders, including from outside Drill, through a small Java interface.
- A decoder only ever handles its own protocol. Traffic that merely uses the protocol's port is left alone.
- Errors never stop a file from being analyzed, and every error is visible in the query results.
- Queries that do not ask for decoded data pay nothing for it.

## Columns

Three columns are added to the packet schema of both readers and to the session schema
(`sessionizeTCPStreams`):

Column | Type | Contents
-------|------|---------
`parsed_protocol` | VARCHAR | Name of the decoder that handled the row, such as `dns`. Null if none did.
`parsed_data` | MAP | One sub-map per registered decoder. Only the sub-map named by `parsed_protocol` is filled.
`decode_error` | VARCHAR | Why the row could not be fully decoded. Null when there was no problem.

```sql
SELECT parsed_data.dns.questions[0].name, parsed_data.dns.answers
FROM dfs.`capture.pcapng`
WHERE parsed_protocol = 'dns';

SELECT parsed_data.smtp.mail_from, parsed_data.smtp.rcpt_to
FROM table(dfs.`mail.pcapng` (type => 'pcapng', sessionizeTCPStreams => true))
WHERE parsed_protocol = 'smtp';

SELECT decode_error, count(*) FROM dfs.`capture.pcapng`
WHERE decode_error IS NOT NULL GROUP BY decode_error;
```

Each protocol has its own sub-map so that field names cannot collide (HTTP, SIP and RTSP all have a
`method`) and so that fields keep real types: integers, timestamps, arrays and repeated maps.

Decoding runs only when `parsed_protocol` or `parsed_data` is projected.

## Interfaces

Package `org.apache.drill.exec.store.pcap.protocol`, used by both readers.

```java
public interface ProtocolDecoder {
  /** Value of parsed_protocol and the name of this decoder's sub-map. Lower case, unique. */
  String protocol();

  /** When several decoders accept the same data, higher priority is tried first. */
  default int priority() { return 0; }
}

public interface PacketProtocolDecoder<T> extends ProtocolDecoder {
  /** Fields of parsed_data.<protocol> for packet rows. */
  void defineSchema(SchemaBuilder fields);

  /** Cheap pre-check, typically transport and port. */
  boolean accepts(Packet packet);

  /**
   * Fully validates and parses the payload.
   * @return null if the payload is not this protocol; throw if it is this protocol but malformed
   */
  T parse(Packet packet, byte[] payload, DecoderContext context);

  /** Writes a successful parse. Must not fail on a value returned by parse. */
  void write(T parsed, TupleWriter fields);
}

public interface SessionProtocolDecoder<T> extends ProtocolDecoder {
  /** Fields of parsed_data.<protocol> for session rows. */
  void defineSchema(SchemaBuilder fields);

  boolean accepts(TcpSession session);

  T parse(TcpStream fromClient, TcpStream fromServer, DecoderContext context);

  void write(T parsed, TupleWriter fields);
}
```

`parse` and `write` are separate so a decoder can never leave a half-written sub-map: everything is
validated before anything is written.

A protocol that is useful both per packet and per session (HTTP) has one class of each kind, sharing
its parsing code. Within a query the mode is fixed, so `parsed_data` holds the packet decoders' sub-maps
in packet mode and the session decoders' sub-maps in session mode.

`DecoderContext` gives decoders the format options they need, currently `exposeCredentials`.

### Registration

`ProtocolDecoders` loads every `PacketProtocolDecoder` and `SessionProtocolDecoder` once through
`java.util.ServiceLoader`. Built-in decoders are listed in
`META-INF/services/org.apache.drill.exec.store.pcap.protocol.PacketProtocolDecoder` (and the
`SessionProtocolDecoder` equivalent) in the plugin jar. A third-party decoder is a jar on Drill's
classpath with its own `META-INF/services` entries; no change to Drill is needed.

If two decoders of the same kind declare the same `protocol()` name, the one loaded first is kept and
the other is ignored with a logged warning naming both classes.

### Matching

For each packet or session, the registry tries the decoders whose `accepts` returns true, highest
priority first:

- `parse` returns a value: that decoder handles the row. Stop.
- `parse` returns null: not this protocol. Try the next decoder.
- `parse` throws: the decoder recognized its protocol but could not parse it. `parsed_protocol` is set to
  that decoder's protocol, its sub-map stays empty, `decode_error` explains, and no other decoder is tried.

## Data Flow

### Packet mode

After the reader decodes a packet, and if a decoder column is projected, it passes the packet and the
bytes for the decoder's layer to the registry:

- TCP and UDP: the transport payload.
- ICMP and ICMPv6: the bytes after the IP header.
- ARP: the bytes after the link-layer header.

`Packet` gains two accessors for the last two.

### Session mode

When `TcpSessionizer` writes a session, at its close or at end of file, it passes the session to the
registry. A `TcpStream` for each direction is built only when a decoder column is projected.

**TcpStream** is the reassembled byte stream of one direction:

- Segments are ordered by sequence number relative to the first segment seen, so sequence wraparound
  is handled.
- Retransmitted and overlapping bytes are kept once.
- Missing ranges are recorded. `hasGaps()` and `firstGap()` let decoders stop at a gap rather than parse
  across it.

**Client and server:** the client is the side that sent the SYN without ACK. If the handshake was not
captured, the endpoint with the lower port is taken as the server.

### Repeated values

Lists use Drill repeated types: DNS answers, SMTP recipients, and the requests of a keep-alive HTTP
connection are arrays or repeated maps, for example `parsed_data.http.exchanges[0].uri`.

## Credentials

Decoders for protocols that carry cleartext credentials (FTP, POP3, IMAP, SMTP AUTH, HTTP Basic) write
`username` and `password_present` by default. The password itself is written to a `password` field only
when the format option `exposeCredentials` is true. Credentials sent after a protocol switches to TLS are
not visible to any decoder.

## Error Handling

The rule: **an error never stops a file from being analyzed, and every error is visible in the results.**

Errors are reported in the `decode_error` column of the row they affect, prefixed with where they
happened (`dns: ...`, `packet: ...`, `file: ...`). A row with several problems lists them separated by
`; `. Each reader also logs one warning per file with error counts by kind.

Source | Result
-------|-------
Decoder `parse` throws | Row kept; `parsed_protocol` set, sub-map empty, `decode_error` = `<protocol>: <message>`
Decoder `write` throws | Row kept with whatever was written; `decode_error` = `<protocol>: write failed: <message>`. This is a decoder bug.
Malformed packet headers (for example an invalid IPv4 header length, which used to fail the query) | Row kept with the fields that could be read; `is_corrupt` true where the schema has it; `decode_error` = `packet: <message>`
PCAP-NG packet referencing an undefined interface | Row kept, link type unknown so no IP decoding; `decode_error` explains
PCAP-NG interface with an unsupported `if_tsresol` | Its packets use microseconds; `decode_error` explains
PCAP-NG block with an invalid captured length | Block skipped; an error row is written
Structural damage that makes the rest of the file unreadable (invalid block length, truncated block or header, unreadable file, not a PCAP or PCAP-NG file) | Rows read so far are kept; reading the file stops; an error row is written
Session stream gap that stops a decoder early | Fields parsed before the gap are kept; `decode_error` = `<protocol>: stopped at missing data in <direction> stream at byte <n>`

An **error row** has `decode_error` set and every other column null. To allow this, the PCAP-NG packet
columns that are currently `REQUIRED` (`packet_timestamp`, `packet_length`, `captured_length`, `interface_id`,
`link_type`) become nullable. Queries that count packets can exclude error rows with
`WHERE decode_error IS NULL` or count them with `WHERE decode_error IS NOT NULL`.

Snapshot-length truncation is normal capture behavior, not an error: payloads are bounded by the
captured length without reporting anything.

### Bounded work

Capture files are untrusted input. Every decoder must bound its work so a crafted packet cannot hang a
reader or exhaust memory:

- Follow at most 16 DNS compression pointers per name, and never a pointer to its own position or later.
- Cap repeated items: 64 headers or records per message, 64 list entries per field.
- Cap strings at 4 KB, truncating longer values.
- Never allocate based on a length field without checking it against the bytes available.
- Session decoders stop after 1,000 messages per session.

Values over a cap are truncated and noted in `decode_error`; they are never a reason to fail.

## Testing

Each decoder has:

- Unit tests of `parse` on byte arrays, without a Drill cluster.
- A query test on a fixture covering a valid exchange, a different protocol on the decoder's port (must
  be left undecoded), and a truncated or malformed message (must produce `decode_error`, not a failed
  query).

Fixtures are generated by scripts or taken from public sample captures, and their expected values are
cross-checked with an independent decoder (scapy or Wireshark).

The framework is tested with a test-only decoder registered under `src/test/resources/META-INF/services`,
which proves the third-party path end to end, and with fixtures that exercise each error-handling row
above.

## Rollout

All phases are part of DRILL-8556.

Phase | Contents
------|---------
1. Framework | Interfaces, registry, `TcpStream`, the three columns, `exposeCredentials`, the error handling above for both readers. Decoders: DNS (including mDNS and LLMNR) per packet, HTTP/1.x per packet and per session.
2. Packet decoders | DHCP, DHCPv6, NTP, TLS ClientHello (SNI, ALPN, JA3), SSDP, SIP, Syslog, NetBIOS name service, TFTP, STUN, RADIUS, SNMP, ICMP and ICMPv6, ARP
3. Session decoders | SMTP, POP3, IMAP, FTP control channel, SSH banners (HASSH if practical), TLS server side (certificate subject, issuer, validity; JA3S), DNS over TCP
Later | Kerberos, SMB, QUIC, HTTP/2. These need much more parsing per protocol and are encrypted or deeply nested.

## Phase 1 Decoder Fields

### `dns` (packet)

Ports 53, 5353 (mDNS) and 5355 (LLMNR), over UDP and single-segment TCP (DNS over TCP across segments is
a phase 3 session decoder). A payload is DNS only if the header counts are consistent with its length and
every question and record parses.

Field | Type
------|-----
`transaction_id` | INT
`is_response` | BIT
`opcode` | INT
`rcode` | INT
`authoritative`, `truncated`, `recursion_desired`, `recursion_available` | BIT
`questions` | repeated map: `name` VARCHAR, `type` VARCHAR (`A`, `AAAA`, ... or the number), `class` INT
`answers`, `authorities`, `additionals` | repeated map: `name` VARCHAR, `type` VARCHAR, `class` INT, `ttl` BIGINT, `data` VARCHAR (address, name, or text rendering of the record)

### `http` (packet)

TCP. A payload is HTTP only if it starts with a request line (`<METHOD> <target> HTTP/1.x`) or a status
line (`HTTP/1.x <code> <reason>`). The message's start line and the headers present in that segment are
parsed.

Field | Type
------|-----
`is_request` | BIT
`method`, `uri`, `version` | VARCHAR
`status_code` | INT
`reason` | VARCHAR
`host`, `user_agent`, `content_type`, `referer` | VARCHAR
`content_length` | BIGINT
`headers` | repeated map: `name`, `value` VARCHAR
`username` | VARCHAR (HTTP Basic)
`password_present` | BIT
`password` | VARCHAR (only with `exposeCredentials`)

### `http` (session)

Every request in the client stream and response in the server stream, paired in order.

Field | Type
------|-----
`exchanges` | repeated map: the request fields (`method`, `uri`, `version`, `host`, `user_agent`, `referer`, `username`, `password_present`, `password`, `request_headers`) and the response fields (`status_code`, `reason`, `content_type`, `content_length`, `response_headers`)

Bodies are skipped using `Content-Length` or chunked encoding, so later messages on a keep-alive
connection are found. Bodies themselves are not returned; `data_from_originator` and `data_from_remote`
already carry the raw bytes.

## Phase 2 Decoder Fields

Each decoder matches on the ports listed, then validates the payload as described in Matching: other
traffic on those ports is left undecoded. Lists are capped at 64 entries and strings at 4096 characters,
with a warning in `decode_error` when a cap is hit.

### `icmp` (packet)

ICMP and ICMPv6 packets (the bytes after the IP header).

Field | Type
------|-----
`version` | INT (4 for ICMP, 6 for ICMPv6)
`type`, `code` | INT
`type_name`, `code_name` | VARCHAR, such as `echo_request` or `port_unreachable`
`identifier`, `sequence` | INT (echo messages)
`mtu` | INT (fragmentation needed, packet too big)
`gateway` | VARCHAR (redirect)
`target_address`, `destination_address` | VARCHAR (neighbor discovery, ICMPv6 redirect)
`original_src_ip`, `original_dst_ip`, `original_protocol`, `original_src_port`, `original_dst_port` | VARCHAR, INT: the header quoted by an error message

### `arp` (packet)

ARP packets (the bytes after the link header).

Field | Type
------|-----
`hardware_type`, `protocol_type`, `operation` | INT
`operation_name` | VARCHAR (`request`, `reply`, `rarp_request`, ...)
`sender_mac`, `sender_ip`, `target_mac`, `target_ip` | VARCHAR
`is_gratuitous`, `is_probe` | BIT

### `ntp` (packet)

UDP 123.

Field | Type
------|-----
`leap_indicator`, `version`, `stratum`, `poll`, `precision` | INT
`mode` | VARCHAR (`client`, `server`, `broadcast`, `symmetric_active`, ...)
`root_delay`, `root_dispersion` | FLOAT8 (seconds)
`reference_id` | VARCHAR (IP address, or a code such as `GPS` for stratum 1)
`reference_time`, `origin_time`, `receive_time`, `transmit_time` | TIMESTAMP
`request_code` | INT (mode 6 and 7 control messages)

### `syslog` (packet)

UDP 514. RFC 5424 messages and the older BSD (RFC 3164) format.

Field | Type
------|-----
`facility`, `severity` | INT
`facility_name`, `severity_name` | VARCHAR
`version` | INT (RFC 5424 only)
`timestamp_text` | VARCHAR, as sent (the formats have no year or zone in common)
`hostname`, `app_name`, `proc_id`, `msg_id`, `structured_data`, `message` | VARCHAR

### `ssdp` (packet)

UDP 1900.

Field | Type
------|-----
`method` | VARCHAR (`M-SEARCH`, `NOTIFY`)
`status_code` | INT (responses)
`st`, `nt`, `nts`, `usn`, `location`, `server`, `user_agent`, `man`, `cache_control` | VARCHAR
`mx` | INT
`headers` | repeated map: `name`, `value` VARCHAR

### `sip` (packet)

UDP or single-segment TCP 5060.

Field | Type
------|-----
`is_request` | BIT
`method`, `request_uri`, `reason` | VARCHAR
`status_code` | INT
`from_address`, `to_address`, `call_id`, `cseq`, `user_agent`, `contact`, `content_type` | VARCHAR
`via` | VARCHAR array
`content_length` | BIGINT
`username` | VARCHAR (from an `Authorization` header)
`password_present` | BIT (always false: SIP digest authentication never sends a cleartext password)
`headers` | repeated map: `name`, `value` VARCHAR

### `dhcp` (packet)

UDP 67 and 68.

Field | Type
------|-----
`op`, `message_type` | VARCHAR (`DISCOVER`, `OFFER`, `REQUEST`, `ACK`, ...)
`transaction_id`, `lease_time` | BIGINT
`broadcast` | BIT
`client_mac`, `client_ip`, `your_ip`, `server_ip`, `relay_ip` | VARCHAR
`server_name`, `boot_file`, `hostname`, `requested_ip`, `server_id`, `subnet_mask`, `vendor_class`, `domain_name` | VARCHAR
`routers`, `dns_servers` | VARCHAR array
`parameter_request_list` | INT array
`options` | repeated map: `code` INT, `value` VARCHAR (every option, as hex)

### `dhcpv6` (packet)

UDP 546 and 547. Relay messages are unwrapped.

Field | Type
------|-----
`message_type`, `relayed_message_type` | VARCHAR
`hop_count`, `transaction_id`, `status_code` | INT
`link_address`, `peer_address` | VARCHAR (relay messages)
`client_duid`, `server_duid`, `status_message`, `fqdn` | VARCHAR
`ia_addresses`, `ia_prefixes`, `dns_servers`, `domain_list` | VARCHAR array
`option_request_list` | INT array
`options` | repeated map: `code` INT, `value` VARCHAR

### `tftp` (packet)

UDP 69. Transfers move to other ports after the first packet, so only requests and replies to port 69 are
decoded.

Field | Type
------|-----
`opcode` | VARCHAR (`RRQ`, `WRQ`, `DATA`, `ACK`, `ERROR`, `OACK`)
`filename`, `mode` | VARCHAR
`block`, `data_length`, `error_code` | INT
`error_message` | VARCHAR
`options` | repeated map: `name`, `value` VARCHAR

### `netbios_ns` (packet)

UDP 137.

Field | Type
------|-----
`transaction_id`, `rcode` | INT
`is_response`, `broadcast` | BIT
`opcode` | VARCHAR
`questions` | repeated map: `name` VARCHAR, `suffix` INT, `suffix_name` VARCHAR, `type` VARCHAR
`answers`, `authorities`, `additionals` | repeated map: the question fields plus `ttl` BIGINT, `addresses` VARCHAR array, `node_type` VARCHAR, `is_group` BIT, `names` VARCHAR array and `mac_address` VARCHAR (node status replies)

### `radius` (packet)

UDP 1812, 1813, 1645 and 1646.

Field | Type
------|-----
`code`, `identifier` | INT
`code_name` | VARCHAR (`Access-Request`, `Accounting-Request`, ...)
`authenticator` | VARCHAR (hex)
`username` | VARCHAR
`password_present` | BIT (the `User-Password` attribute is encrypted with the shared secret and never decoded)
`nas_ip_address`, `nas_identifier`, `calling_station_id`, `called_station_id`, `framed_ip_address`, `acct_status_type`, `acct_session_id`, `reply_message` | VARCHAR
`nas_port` | BIGINT
`attributes` | repeated map: `type` INT, `value` VARCHAR

### `snmp` (packet)

UDP 161 and 162. Versions 1, 2c and 3; v3 scoped PDUs are decoded only when not encrypted.

Field | Type
------|-----
`version`, `pdu_type` | VARCHAR
`community_present` | BIT
`community` | VARCHAR (only with `exposeCredentials`)
`request_id`, `specific_trap`, `time_stamp`, `msg_id` | BIGINT
`error_status`, `error_index`, `non_repeaters`, `max_repetitions`, `generic_trap` | INT
`enterprise`, `agent_address` | VARCHAR (v1 traps)
`msg_user_name`, `security_level`, `engine_id` | VARCHAR (v3)
`encrypted` | BIT (v3 privacy)
`varbinds` | repeated map: `oid`, `value_type`, `value` VARCHAR

### `tls` (packet)

TCP 443, 465, 563, 636, 853, 989, 990, 992, 993, 994, 995, 5061 and 8443. ClientHello and ServerHello
messages that fit in one segment (see the `tls` session decoder for the rest).

Field | Type
------|-----
`handshake_type` | VARCHAR (`client_hello`, `server_hello`)
`record_version`, `version`, `session_id`, `sni` | VARCHAR
`supported_versions`, `alpn` | VARCHAR array
`cipher_suites`, `extensions`, `supported_groups`, `ec_point_formats`, `signature_algorithms` | INT array (GREASE values are kept here and dropped from JA3)
`cipher_suite` | INT (ServerHello)
`ja3`, `ja3_hash` | VARCHAR (ClientHello)
`ja3s`, `ja3s_hash` | VARCHAR (ServerHello)

### `stun` (packet)

UDP 3478 and 19302. The magic cookie must be present.

Field | Type
------|-----
`message_class`, `message_method`, `transaction_id` | VARCHAR
`xor_mapped_address`, `mapped_address`, `software`, `realm`, `nonce`, `error_reason`, `username` | VARCHAR
`error_code` | INT
`password_present` | BIT (always false: STUN never sends a cleartext password)
`attributes` | repeated map: `type`, `length` INT

## Phase 3 Decoder Fields

Session decoders read both reassembled directions of a TCP session. A gap in a stream stops parsing that
direction with a warning; what was parsed before it is kept.

### `tls` (session)

The same ports as the `tls` packet decoder. Handshake messages are reassembled across records and segments.

Field | Type
------|-----
`client_version`, `server_version`, `sni`, `alpn_selected`, `cipher_suite_name` | VARCHAR
`client_supported_versions`, `alpn_offered` | VARCHAR array
`cipher_suite`, `certificate_count` | INT
`session_resumed`, `hello_retry_request` | BIT
`certificate_encrypted` | BIT (TLS 1.3, where certificates are not visible)
`certificates` | repeated map: `subject`, `issuer`, `serial`, `signature_algorithm`, `public_key_algorithm`, `sha256` VARCHAR; `not_before`, `not_after` TIMESTAMP; `subject_alt_names` VARCHAR array; `public_key_bits` INT; `is_self_signed` BIT

### `ftp` (session)

TCP 21. The server stream must start with a 220 greeting or the client stream with `USER` or `AUTH`. Data
channels are separate TCP sessions and are not decoded. Parsing stops when TLS starts.

Field | Type
------|-----
`banner`, `system_type` | VARCHAR
`username` | VARCHAR
`password_present` | BIT
`password` | VARCHAR (only with `exposeCredentials`)
`tls_started` | BIT
`current_directories` | VARCHAR array
`transfers` | repeated map: `command`, `path` VARCHAR, `reply_code` INT, `data_address` VARCHAR (`ip:port`; `:port` for EPSV, whose host is the server's control address)
`commands` | repeated map: `command`, `argument` VARCHAR (the PASS argument is `***` unless `exposeCredentials`)
`replies` | repeated map: `code` INT, `text` VARCHAR

### `ssh` (session)

TCP 22 and 2222. SSH 2.0 (and 1.99) identification strings and the first KEXINIT in each direction.

Field | Type
------|-----
`client_version`, `server_version`, `client_software`, `server_software`, `client_comments`, `server_comments` | VARCHAR
`client_kex_algorithms`, `client_host_key_algorithms`, `client_ciphers`, `client_macs`, `client_compression` | VARCHAR array (client to server lists)
`server_kex_algorithms`, `server_host_key_algorithms`, `server_ciphers`, `server_macs`, `server_compression` | VARCHAR array (server to client lists)
`hassh`, `hassh_string`, `hassh_server`, `hassh_server_string` | VARCHAR

### `dns` (session)

TCP 53: length-prefixed DNS messages, such as zone transfers and large responses.

Field | Type
------|-----
`queries` | repeated map: `transaction_id` INT, `name`, `type` VARCHAR
`answers` | repeated map: `transaction_id` INT, `name`, `type` VARCHAR, `ttl` BIGINT, `data` VARCHAR
`client_message_count`, `server_message_count` | INT
`is_zone_transfer` | BIT

## Writing a Decoder

1. Implement `PacketProtocolDecoder<T>` or `SessionProtocolDecoder<T>`, where `T` is a small class
   holding the parsed fields.
2. In `accepts`, check transport and ports only.
3. In `parse`, validate everything before returning a value. Return null for data that is not your
   protocol; throw for data that is your protocol but malformed. Follow the bounded-work rules.
4. In `write`, write only fields declared in `defineSchema`, and skip null values (the sub-map's
   fields are nullable).
5. List the class in the matching `META-INF/services` file.
6. Add the tests described above.

## Implementation Notes

Differences from the design above, recorded during implementation:

- The two `Packet` accessors for ICMP and ARP bytes arrive with the phase 2 ICMP and ARP decoders; no phase 1
  decoder needs them.
- DNS separates "not DNS" from "malformed DNS" by confidence: a payload whose header is implausible, or whose
  first question (or first record, when there are no questions) does not parse, is not DNS and is left
  undecoded. Once one question or record has parsed, a later failure is reported in `decode_error`, for
  example `dns: truncated answer 1: ...`.
- Every field of `parsed_data` is created with sparse vector sizing (one expected element per array, a small
  VARCHAR width). Drill otherwise reserves room for a full batch of values in each of the many mostly empty
  decoder fields, which exhausted the batch memory budget.
- Two Drill scan framework bugs had to be fixed for decoder fields that are lists: a repeated map inside a map
  lost its offsets in the scan output (`OutputBatchBuilder`), and an array index inside a map was parsed as an
  extra array dimension (`ScanProjectionParser`), which rejected projections such as
  `parsed_data.dns.questions[0].name`.
- With many decoders the sparse sizing was not enough: harvesting a full batch filled the unwritten decoder
  fields, which asked for more memory than the batch budget allowed and failed the query. `ResultSetLoaderImpl`
  now lets those fills through while harvesting (`TestResultSetLoaderLimits#testHarvestFillsUnwrittenColumnsOverBudget`).
- The classic PCAP reader could not read gzip files: a refill overwrote unread bytes, and packets kept for
  sessions pointed into the reused buffer. Refills now append after unread bytes and each packet copies its
  record. A read failure part way through a file keeps the packets already read and adds an error row.
- The packet decoders for phases 2 and 3 read ICMP from `Packet.getIpPayload()` and ARP from
  `Packet.getLinkPayload()`.
- Session decoders receive only the two reassembled streams, so FTP cannot fill in the host of an EPSV data
  address.
- Field names avoid words Drill reserves after a dot (`from`, `timestamp`, `date`, `group`), so every field can
  be queried without quotes: SIP uses `from_address` and `to_address`, syslog `timestamp_text`.
  `TestDecoderFieldNames` checks this for every registered decoder.
