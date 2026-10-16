/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.pcapng;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.store.pcap.PcapFormatUtils;
import org.apache.drill.exec.vector.accessor.ScalarWriter;


public abstract class PcapColumn {

  private static final Map<String, PcapColumn> columns = new LinkedHashMap<>();
  private static final Map<String, PcapColumn> summary_columns = new LinkedHashMap<>();
  public static final String DUMMY_NAME = "dummy";
  public static final String PATH_NAME = "path";

  static {
    // Basic
    columns.put("packet_timestamp", new PcapTimestamp());
    columns.put("packet_length", new PcapPacketLength());
    columns.put("type", new PcapType());
    columns.put("src_ip", new PcapSrcIp());
    columns.put("dst_ip", new PcapDstIp());
    columns.put("src_port", new PcapSrcPort());
    columns.put("dst_port", new PcapDstPort());
    columns.put("src_mac_address", new PcapSrcMac());
    columns.put("dst_mac_address", new PcapDstMac());
    columns.put("tcp_session", new PcapTcpSession());
    columns.put("tcp_ack", new PcapTcpAck());
    columns.put("tcp_flags", new PcapTcpFlags());
    columns.put("tcp_flags_ns", new PcapTcpFlagsNs());
    columns.put("tcp_flags_cwr", new PcapTcpFlagsCwr());
    columns.put("tcp_flags_ece", new PcapTcpFlagsEce());
    columns.put("tcp_flags_ece_ecn_capable", new PcapTcpFlagsEceEcnCapable());
    columns.put("tcp_flags_ece_congestion_experienced", new PcapTcpFlagsEceCongestionExperienced());
    columns.put("tcp_flags_urg", new PcapTcpFlagsUrg());
    columns.put("tcp_flags_ack", new PcapTcpFlagsAck());
    columns.put("tcp_flags_psh", new PcapTcpFlagsPsh());
    columns.put("tcp_flags_rst", new PcapTcpFlagsRst());
    columns.put("tcp_flags_syn", new PcapTcpFlagsSyn());
    columns.put("tcp_flags_fin", new PcapTcpFlagsFin());
    columns.put("tcp_parsed_flags", new PcapTcpParsedFlags());
    columns.put("packet_data", new PcapPacketData());
    // Interface and Enhanced Packet Block metadata
    columns.put("captured_length", new PcapCapturedLength());
    columns.put("interface_id", new PcapInterfaceId());
    columns.put("interface_name", new PcapInterfaceName());
    columns.put("link_type", new PcapLinkType());
    columns.put("comment", new PcapComment());
    columns.put("direction", new PcapDirection());
    columns.put("reception_type", new PcapReceptionType());
    columns.put("fcs_length", new PcapFcsLength());
    columns.put("drop_count", new PcapDropCount());
    columns.put("packet_hash", new PcapPacketHash());

    // Extensions
    summary_columns.put("path", new PcapStatPath());
    summary_columns.put("comment", new PcapComment());
    // Section Header Block
    addStat("shb_hardware", MinorType.VARCHAR);
    addStat("shb_os", MinorType.VARCHAR);
    addStat("shb_userappl", MinorType.VARCHAR);
    // Interface Description Block
    addStat("if_name", MinorType.VARCHAR);
    addStat("if_description", MinorType.VARCHAR);
    addStat("if_ipv4addr", MinorType.VARCHAR);
    addStat("if_ipv6addr", MinorType.VARCHAR);
    addStat("if_macaddr", MinorType.VARCHAR);
    addStat("if_euiaddr", MinorType.VARCHAR);
    addStat("if_speed", MinorType.BIGINT);
    addStat("if_tsresol", MinorType.INT);
    addStat("if_tzone", MinorType.INT);
    addStat("if_os", MinorType.VARCHAR);
    addStat("if_fcslen", MinorType.INT);
    addStat("if_tsoffset", MinorType.BIGINT);
    // Name Resolution Block
    addStat("ns_dnsname", MinorType.VARCHAR);
    addStat("ns_dnsip4addr", MinorType.VARCHAR);
    addStat("ns_dnsip6addr", MinorType.VARCHAR);
    // Interface Statistics Block
    addStat("isb_starttime", MinorType.TIMESTAMP);
    addStat("isb_endtime", MinorType.TIMESTAMP);
    addStat("isb_ifrecv", MinorType.BIGINT);
    addStat("isb_ifdrop", MinorType.BIGINT);
    addStat("isb_filteraccept", MinorType.BIGINT);
    addStat("isb_osdrop", MinorType.BIGINT);
    addStat("isb_usrdeliv", MinorType.BIGINT);
  }

  private static void addStat(String name, MinorType type) {
    summary_columns.put(name, new PcapStat(name, type));
  }

  abstract MajorType getType();

  abstract void process(PcapngBlock block, ScalarWriter writer);

  private static void setString(ScalarWriter writer, String value) {
    if (value != null) {
      writer.setString(value);
    }
  }

  public static Map<String, PcapColumn> getColumns() {
    return Collections.unmodifiableMap(columns);
  }

  public static Map<String, PcapColumn> getSummaryColumns() {
    return Collections.unmodifiableMap(summary_columns);
  }

  static class PcapDummy extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) { }
  }

  static class PcapStatPath extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.VARCHAR);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) { }
  }

  static class PcapTimestamp extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.TIMESTAMP);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      writer.setTimestamp(block.timestamp);
    }
  }

  static class PcapPacketLength extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      writer.setInt(block.originalLength);
    }
  }

  static class PcapType extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.VARCHAR);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setString(block.packet.getPacketType());
      }
    }
  }

  static class PcapSrcIp extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.VARCHAR);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        setString(writer, block.packet.getSourceIpAddressString());
      }
    }
  }

  static class PcapDstIp extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.VARCHAR);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        setString(writer, block.packet.getDestinationIpAddressString());
      }
    }
  }

  static class PcapSrcPort extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null && (block.packet.isTcpPacket() || block.packet.isUdpPacket())) {
        writer.setInt(block.packet.getSrc_port());
      }
    }
  }

  static class PcapDstPort extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null && (block.packet.isTcpPacket() || block.packet.isUdpPacket())) {
        writer.setInt(block.packet.getDst_port());
      }
    }
  }

  static class PcapSrcMac extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.VARCHAR);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        setString(writer, block.packet.getEthernetSource());
      }
    }
  }

  static class PcapDstMac extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.VARCHAR);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        setString(writer, block.packet.getEthernetDestination());
      }
    }
  }

  static class PcapTcpSession extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.BIGINT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setLong(block.packet.getSessionHash());
      }
    }
  }

  static class PcapTcpAck extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setInt(block.packet.getAckNumber());
      }
    }
  }

  static class PcapTcpFlags extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setInt(block.packet.getFlags());
      }
    }
  }

  static class PcapTcpFlagsNs extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setBoolean((block.packet.getFlags() & 0x100) != 0);
      }
    }
  }

  static class PcapTcpFlagsCwr extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setBoolean((block.packet.getFlags() & 0x80) != 0);
      }
    }
  }

  static class PcapTcpFlagsEce extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setBoolean((block.packet.getFlags() & 0x40) != 0);
      }
    }
  }

  static class PcapTcpFlagsEceEcnCapable extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setBoolean((block.packet.getFlags() & 0x42) == 0x42);
      }
    }
  }

  static class PcapTcpFlagsEceCongestionExperienced extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setBoolean((block.packet.getFlags() & 0x42) == 0x40);
      }
    }
  }

  static class PcapTcpFlagsUrg extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setBoolean((block.packet.getFlags() & 0x20) != 0);
      }
    }
  }

  static class PcapTcpFlagsAck extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setBoolean((block.packet.getFlags() & 0x10) != 0);
      }
    }
  }

  static class PcapTcpFlagsPsh extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setBoolean((block.packet.getFlags() & 0x8) != 0);
      }
    }
  }

  static class PcapTcpFlagsRst extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setBoolean((block.packet.getFlags() & 0x4) != 0);
      }
    }
  }

  static class PcapTcpFlagsSyn extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setBoolean((block.packet.getFlags() & 0x2) != 0);
      }
    }
  }

  static class PcapTcpFlagsFin extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setBoolean((block.packet.getFlags() & 0x1) != 0);
      }
    }
  }

  static class PcapTcpParsedFlags extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.VARCHAR);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setString(block.packet.getParsedFlags());
      }
    }
  }

  static class PcapPacketData extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.VARCHAR);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.packet != null) {
        writer.setString(PcapFormatUtils.parseBytesToASCII(block.data));
      }
    }
  }

  // Interface and Enhanced Packet Block metadata

  static class PcapCapturedLength extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      writer.setInt(block.capturedLength);
    }
  }

  static class PcapInterfaceId extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      writer.setInt(block.interfaceId);
    }
  }

  static class PcapInterfaceName extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.VARCHAR);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      setString(writer, block.interfaceName);
    }
  }

  /**
   * link_type: LINKTYPE_ value of the packet's interface, see https://www.tcpdump.org/linktypes.html
   */
  static class PcapLinkType extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      writer.setInt(block.linkType);
    }
  }

  /**
   * comment: opt_comment of the block; multiple comments are newline separated
   */
  static class PcapComment extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.VARCHAR);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      setString(writer, block.comment);
    }
  }

  /**
   * direction: inbound or outbound, from epb_flags bits 0-1
   */
  static class PcapDirection extends PcapColumn {
    private static final String[] VALUES = {null, "inbound", "outbound", null};

    @Override
    MajorType getType() {
      return Types.optional(MinorType.VARCHAR);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.flags != null) {
        setString(writer, VALUES[block.flags & 0x3]);
      }
    }
  }

  /**
   * reception_type: unicast, multicast, broadcast or promiscuous, from epb_flags bits 2-4
   */
  static class PcapReceptionType extends PcapColumn {
    private static final String[] VALUES = {null, "unicast", "multicast", "broadcast", "promiscuous", null, null, null};

    @Override
    MajorType getType() {
      return Types.optional(MinorType.VARCHAR);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.flags != null) {
        setString(writer, VALUES[(block.flags >> 2) & 0x7]);
      }
    }
  }

  /**
   * fcs_length: frame check sequence length in octets, from epb_flags bits 5-8
   */
  static class PcapFcsLength extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.INT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.flags != null && ((block.flags >> 5) & 0xF) != 0) {
        writer.setInt((block.flags >> 5) & 0xF);
      }
    }
  }

  /**
   * drop_count: packets lost between this packet and the preceding one (epb_dropcount)
   */
  static class PcapDropCount extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.BIGINT);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      if (block.dropCount != null) {
        writer.setLong(block.dropCount);
      }
    }
  }

  /**
   * packet_hash: epb_hash as algorithm:hex, such as md5:9e107d9d372bb6826bd81d3542a419d6
   */
  static class PcapPacketHash extends PcapColumn {

    @Override
    MajorType getType() {
      return Types.optional(MinorType.VARCHAR);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      setString(writer, block.hash);
    }
  }

  /**
   * A stat column, read from the options the reader decoded from a
   * Section Header, Interface Description, Name Resolution or Interface
   * Statistics Block. Null when the block does not have the option.
   */
  static class PcapStat extends PcapColumn {
    private final String name;
    private final MinorType type;

    PcapStat(String name, MinorType type) {
      this.name = name;
      this.type = type;
    }

    @Override
    MajorType getType() {
      return Types.optional(type);
    }

    @Override
    void process(PcapngBlock block, ScalarWriter writer) {
      Object value = block.stats == null ? null : block.stats.get(name);
      if (value == null) {
        return;
      }
      switch (type) {
        case VARCHAR:
          writer.setString((String) value);
          break;
        case INT:
          writer.setInt((Integer) value);
          break;
        case BIGINT:
          writer.setLong((Long) value);
          break;
        case TIMESTAMP:
          writer.setTimestamp((Instant) value);
          break;
        default:
          throw new IllegalStateException("Unsupported stat column type " + type);
      }
    }
  }
}
