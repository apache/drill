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
package org.apache.drill.exec.store.pcapng.schema;

import fr.bmartel.pcapdecoder.structure.types.inter.IEnhancedPacketBLock;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.store.pcapng.decoder.PacketDecoder;
import org.apache.drill.exec.vector.ValueVector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.drill.exec.store.pcap.PcapFormatUtils.parseBytesToASCII;
import static org.apache.drill.exec.store.pcapng.schema.Util.setNullableLongColumnValue;

public class Schema {

  private final static Map<String, Column> columns = new HashMap<>();

  static {
    columns.put("timestamp", new TimestampImpl());
    columns.put("packet_length", new PacketLenImpl());
    columns.put("type", new TypeImpl());
    columns.put("src_ip", new SrcIpImpl());
    columns.put("dst_ip", new DstIpImpl());
    columns.put("src_port", new SrcPortImpl());
    columns.put("dst_port", new DstPortImpl());
    columns.put("src_mac_address", new SrcMacImpl());
    columns.put("dst_mac_address", new DstMacImpl());
    columns.put("tcp_session", new TcpSessionImpl());
    columns.put("tcp_ack", new TcpAckImpl());
    columns.put("tcp_flags", new TcpFlags());
    columns.put("tcp_flags_ns", new TcpFlagsNsImpl());
    columns.put("tcp_flags_cwr", new TcpFlagsCwrImpl());
    columns.put("tcp_flags_ece", new TcpFlagsEceImpl());
    columns.put("tcp_flags_ece_ecn_capable", new TcpFlagsEceEcnCapableImpl());
    columns.put("tcp_flags_ece_congestion_experienced", new TcpFlagsEceCongestionExperiencedImpl());
    columns.put("tcp_flags_urg", new TcpFlagsUrgIml());
    columns.put("tcp_flags_ack", new TcpFlagsAckImpl());
    columns.put("tcp_flags_psh", new TcpFlagsPshImpl());
    columns.put("tcp_flags_rst", new TcpFlagsRstImpl());
    columns.put("tcp_flags_syn", new TcpFlagsSynImpl());
    columns.put("tcp_flags_fin", new TcpFlagsFinImpl());
    columns.put("tcp_parsed_flags", new TcpParsedFlags());
    columns.put("packet_data", new PacketDataImpl());
  }

  public static Map<String, Column> getColumns() {
    return columns;
  }

  public static Set<String> getColumnsNames() {
    return columns.keySet();
  }

  static class TimestampImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.required(TypeProtos.MinorType.TIMESTAMP);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      Util.setTimestampColumnValue(block.getTimeStamp(), vv, count);
    }
  }

  static class PacketLenImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.required(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      Util.setIntegerColumnValue(block.getPacketLength(), vv, count);
    }
  }

  static class TypeImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.VARCHAR);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableStringColumnValue(packet.getPacketType(), vv, count);
      }
    }
  }

  static class SrcIpImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.VARCHAR);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableStringColumnValue(packet.getSrc_ip().getHostAddress(), vv, count);
      }
    }
  }

  static class DstIpImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.VARCHAR);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableStringColumnValue(packet.getDst_ip().getHostAddress(), vv, count);
      }
    }
  }

  static class SrcPortImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableIntegerColumnValue(packet.getSrc_port(), vv, count);
      }
    }
  }

  static class DstPortImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableIntegerColumnValue(packet.getDst_port(), vv, count);
      }
    }
  }

  static class SrcMacImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.VARCHAR);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {

      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableStringColumnValue(packet.getEthernetSource(), vv, count);
      }
    }
  }

  static class DstMacImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.VARCHAR);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableStringColumnValue(packet.getEthernetDestination(), vv, count);
      }
    }
  }

  static class TcpSessionImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.BIGINT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        setNullableLongColumnValue(packet.getSessionHash(), vv, count);
      }
    }
  }

  static class TcpAckImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableIntegerColumnValue(packet.getAckNumber(), vv, count);
      }
    }
  }

  static class TcpFlags implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableIntegerColumnValue(packet.getFlags(), vv, count);
      }
    }
  }

  static class TcpFlagsNsImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableBooleanColumnValue((packet.getFlags() & 0x100) != 0, vv, count);
      }
    }
  }

  static class TcpFlagsCwrImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableBooleanColumnValue((packet.getFlags() & 0x80) != 0, vv, count);
      }
    }
  }

  static class TcpFlagsEceImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableBooleanColumnValue((packet.getFlags() & 0x40) != 0, vv, count);
      }
    }
  }

  static class TcpFlagsEceEcnCapableImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableBooleanColumnValue((packet.getFlags() & 0x42) == 0x42, vv, count);
      }
    }
  }

  static class TcpFlagsEceCongestionExperiencedImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableBooleanColumnValue((packet.getFlags() & 0x42) == 0x40, vv, count);
      }
    }
  }

  static class TcpFlagsUrgIml implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableBooleanColumnValue((packet.getFlags() & 0x20) != 0, vv, count);
      }
    }
  }

  static class TcpFlagsAckImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableBooleanColumnValue((packet.getFlags() & 0x10) != 0, vv, count);
      }
    }
  }

  static class TcpFlagsPshImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableBooleanColumnValue((packet.getFlags() & 0x8) != 0, vv, count);
      }
    }
  }

  static class TcpFlagsRstImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableBooleanColumnValue((packet.getFlags() & 0x4) != 0, vv, count);
      }
    }
  }

  static class TcpFlagsSynImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableBooleanColumnValue((packet.getFlags() & 0x2) != 0, vv, count);
      }
    }
  }

  static class TcpFlagsFinImpl implements Column {
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.INT);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableBooleanColumnValue((packet.getFlags() & 0x1) != 0, vv, count);
      }
    }
  }

  static class TcpParsedFlags implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.VARCHAR);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableStringColumnValue(packet.getParsedFlags(), vv, count);
      }
    }
  }

  static class PacketDataImpl implements Column {
    @Override
    public TypeProtos.MajorType getMinorType() {
      return Types.optional(TypeProtos.MinorType.VARCHAR);
    }

    @Override
    public void process(IEnhancedPacketBLock block, ValueVector vv, int count) {
      PacketDecoder packet = new PacketDecoder();
      if (packet.readPcapng(block.getPacketData())) {
        Util.setNullableStringColumnValue(parseBytesToASCII(block.getPacketData()), vv, count);
      }
    }
  }
}
