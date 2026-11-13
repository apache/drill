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
package org.apache.drill.exec.store.pcap;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.decoder.TcpSession;
import org.apache.drill.exec.store.pcap.protocol.ProtocolColumns;
import org.apache.drill.exec.vector.accessor.ScalarWriter;

/**
 * Groups TCP packets into sessions and writes one row per session, using the
 * sessionized schema of {@link org.apache.drill.exec.store.pcap.schema.Schema},
 * when the session closes. Shared by the PCAP and PCAP-NG readers.
 */
public class TcpSessionizer {
  // In order of each session's first packet
  private final Map<Long, TcpSession> openSessions = new LinkedHashMap<>();
  // Sessions already written, so the ACKs that trail a FIN do not start a new one
  private final Set<Long> closedSessions = new HashSet<>();
  private final RowSetLoader rowWriter;
  private final ProtocolColumns protocolColumns;
  private final ScalarWriter srcMacAddressWriter;
  private final ScalarWriter dstMacAddressWriter;
  private final ScalarWriter dstIPWriter;
  private final ScalarWriter srcIPWriter;
  private final ScalarWriter srcPortWriter;
  private final ScalarWriter dstPortWriter;
  private final ScalarWriter sessionStartTimeWriter;
  private final ScalarWriter sessionEndTimeWriter;
  private final ScalarWriter sessionDurationWriter;
  private final ScalarWriter connectionTimeWriter;
  private final ScalarWriter tcpSessionWriter;
  private final ScalarWriter packetCountWriter;
  private final ScalarWriter hostDataWriter;
  private final ScalarWriter remoteDataWriter;
  private final ScalarWriter originPacketCounterWriter;
  private final ScalarWriter remotePacketCounterWriter;
  private final ScalarWriter originDataVolumeWriter;
  private final ScalarWriter remoteDataVolumeWriter;
  private final ScalarWriter isCorruptWriter;
  private final ScalarWriter sessionClosedWriter;

  public TcpSessionizer(RowSetLoader rowWriter, ProtocolColumns protocolColumns) {
    this.rowWriter = rowWriter;
    this.protocolColumns = protocolColumns;
    srcMacAddressWriter = rowWriter.scalar("src_mac_address");
    dstMacAddressWriter = rowWriter.scalar("dst_mac_address");
    dstIPWriter = rowWriter.scalar("dst_ip");
    srcIPWriter = rowWriter.scalar("src_ip");
    srcPortWriter = rowWriter.scalar("src_port");
    dstPortWriter = rowWriter.scalar("dst_port");
    sessionStartTimeWriter = rowWriter.scalar("session_start_time");
    sessionEndTimeWriter = rowWriter.scalar("session_end_time");
    sessionDurationWriter = rowWriter.scalar("session_duration");
    connectionTimeWriter = rowWriter.scalar("connection_time");
    tcpSessionWriter = rowWriter.scalar("tcp_session");
    packetCountWriter = rowWriter.scalar("total_packet_count");
    hostDataWriter = rowWriter.scalar("data_from_originator");
    remoteDataWriter = rowWriter.scalar("data_from_remote");
    originPacketCounterWriter = rowWriter.scalar("packet_count_from_origin");
    remotePacketCounterWriter = rowWriter.scalar("packet_count_from_remote");
    originDataVolumeWriter = rowWriter.scalar("data_volume_from_origin");
    remoteDataVolumeWriter = rowWriter.scalar("data_volume_from_remote");
    isCorruptWriter = rowWriter.scalar("is_corrupt");
    sessionClosedWriter = rowWriter.scalar("session_closed");
  }

  /**
   * Adds a packet to its session, writing the session's row if the packet
   * closes it. Packets other than TCP are ignored.
   */
  public void addPacket(Packet packet) {
    if (!packet.isTcpPacket()) {
      return;
    }
    long sessionId = packet.getSessionHash();
    if (closedSessions.contains(sessionId)) {
      if (!packet.getSynFlag()) {
        return;
      }
      // The same addresses and ports opening a new connection
      closedSessions.remove(sessionId);
    }
    TcpSession session = openSessions.computeIfAbsent(sessionId, TcpSession::new);
    session.addPacket(packet);
    if (session.connectionClosed()) {
      write(session);
      openSessions.remove(sessionId);
      closedSessions.add(sessionId);
    }
  }

  /**
   * Writes the sessions that never saw a FIN or RST, such as ones still open
   * when the capture stopped, with session_closed false. Call at end of file,
   * once per batch until it returns true.
   *
   * @return true when every open session has been written
   */
  public boolean writeOpenSessions() {
    Iterator<TcpSession> sessions = openSessions.values().iterator();
    while (sessions.hasNext() && !rowWriter.isFull()) {
      TcpSession session = sessions.next();
      // Assembles the payload, as a closing handshake would
      session.closeSession();
      write(session);
      sessions.remove();
    }
    return openSessions.isEmpty();
  }

  private void write(TcpSession session) {
    rowWriter.start();

    sessionStartTimeWriter.setTimestamp(session.getSessionStartTime());
    sessionEndTimeWriter.setTimestamp(session.getSessionEndTime());
    sessionDurationWriter.setPeriod(session.getSessionDuration());
    if (session.getConnectionTime() != null) {
      connectionTimeWriter.setPeriod(session.getConnectionTime());
    }

    srcMacAddressWriter.setString(session.getSrcMac());
    dstMacAddressWriter.setString(session.getDstMac());
    srcIPWriter.setString(session.getSrcIP());
    dstIPWriter.setString(session.getDstIP());
    srcPortWriter.setInt(session.getSrcPort());
    dstPortWriter.setInt(session.getDstPort());
    tcpSessionWriter.setLong(session.getSessionID());
    packetCountWriter.setInt(session.getPacketCount());

    originPacketCounterWriter.setInt(session.getPacketCountFromOrigin());
    remotePacketCounterWriter.setInt(session.getPacketCountFromRemote());
    originDataVolumeWriter.setInt(session.getDataFromOriginator().length);
    remoteDataVolumeWriter.setInt(session.getDataFromRemote().length);
    isCorruptWriter.setBoolean(session.hasCorruptedData());
    sessionClosedWriter.setBoolean(session.connectionClosed());

    hostDataWriter.setString(session.getDataFromOriginatorAsString());
    remoteDataWriter.setString(session.getDataFromRemoteAsString());
    protocolColumns.writeSession(session);
    rowWriter.save();
  }
}
