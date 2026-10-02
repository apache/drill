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

import org.apache.drill.exec.store.pcap.decoder.Packet;
import org.apache.drill.exec.store.pcap.decoder.PacketConstants;

import static org.apache.drill.exec.store.pcap.PcapFormatUtils.getByte;
import static org.apache.drill.exec.store.pcap.PcapFormatUtils.getShort;

public class PacketDecoder extends Packet {

  // Link types from https://www.tcpdump.org/linktypes.html
  static final int LINKTYPE_NULL = 0;
  static final int LINKTYPE_ETHERNET = 1;
  static final int LINKTYPE_PPP = 9;
  static final int LINKTYPE_RAW = 101;
  static final int LINKTYPE_IEEE802_11 = 105;
  static final int LINKTYPE_LOOP = 108;
  static final int LINKTYPE_LINUX_SLL = 113;
  static final int LINKTYPE_IEEE802_11_RADIOTAP = 127;
  static final int LINKTYPE_IPV4 = 228;
  static final int LINKTYPE_IPV6 = 229;
  static final int LINKTYPE_LINUX_SLL2 = 276;

  private static final int VLAN_TYPE = 0x8100;
  private static final int QINQ_TYPE = 0x88a8;

  // Offsets of the source and destination MAC addresses, or -1 if the link layer has none
  private int srcMacOffset;
  private int dstMacOffset;

  /**
   * Decodes a packet captured on an interface with the given link type.
   *
   * @return false if the link type or the network protocol is not supported
   */
  public boolean readPcapng(final byte[] raw, final int linkType) {
    this.raw = raw;
    srcMacOffset = -1;
    dstMacOffset = -1;
    int networkOffset = findNetworkLayer(linkType);
    if (networkOffset >= 0 && isArpPacket()) {
      // Typed, but has no IP fields
      return true;
    }
    if (networkOffset < 0 || networkOffset + 20 > raw.length) {
      return false;
    }
    // Packet addresses IP fields relative to a 14-byte Ethernet header, so
    // place a virtual one just before the network layer
    etherOffset = networkOffset - PacketConstants.IP_OFFSET;
    ipOffset = networkOffset;
    try {
      if (isIpV4Packet()) {
        protocol = processIpV4Packet();
        return true;
      } else if (isIpV6Packet()) {
        int tmp = processIpV6Packet();
        if (tmp != -1) {
          protocol = tmp;
        }
        return true;
      } else if (isPPPoV6Packet()) {
        protocol = getByte(raw, etherOffset + 48);
        return true;
      }
    } catch (RuntimeException e) {
      setDecodeError(e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
      etherProtocol = 0;
      transportOffset = -1;
    }
    return false;
  }

  /**
   * Sets {@link #etherProtocol} from the link-layer header.
   *
   * @return offset of the network layer, or -1 if the link type is not supported
   */
  private int findNetworkLayer(int linkType) {
    switch (linkType) {
      case LINKTYPE_ETHERNET:
        if (raw.length < PacketConstants.IP_OFFSET) {
          return -1;
        }
        dstMacOffset = PacketConstants.ETHER_DST_OFFSET;
        srcMacOffset = PacketConstants.ETHER_SRC_OFFSET;
        // Skip 802.1Q and 802.1ad VLAN tags
        int typeOffset = PacketConstants.PACKET_PROTOCOL_OFFSET;
        etherProtocol = getShort(raw, typeOffset);
        while ((etherProtocol == VLAN_TYPE || etherProtocol == QINQ_TYPE) && raw.length >= typeOffset + 6) {
          typeOffset += 4;
          etherProtocol = getShort(raw, typeOffset);
        }
        return typeOffset + 2;
      case LINKTYPE_IEEE802_11:
        return find80211NetworkLayer(0);
      case LINKTYPE_IEEE802_11_RADIOTAP:
        // Radiotap header length is a little-endian short at offset 2
        if (raw.length < 4) {
          return -1;
        }
        return find80211NetworkLayer((raw[2] & 0xff) | (raw[3] & 0xff) << 8);
      case LINKTYPE_RAW:
      case LINKTYPE_IPV4:
      case LINKTYPE_IPV6:
        return ipVersionProtocol(0);
      case LINKTYPE_NULL:
      case LINKTYPE_LOOP:
        // 4-byte address family, in host byte order for NULL. The protocol is
        // read from the IP header instead of mapping per-OS family values.
        return ipVersionProtocol(4);
      case LINKTYPE_LINUX_SLL:
        if (raw.length < 16) {
          return -1;
        }
        etherProtocol = getShort(raw, 14);
        return 16;
      case LINKTYPE_LINUX_SLL2:
        if (raw.length < 20) {
          return -1;
        }
        etherProtocol = getShort(raw, 0);
        return 20;
      case LINKTYPE_PPP:
        // Optional HDLC address/control bytes, then a 2-byte PPP protocol
        int offset = raw.length > 1 && (raw[0] & 0xff) == 0xff && raw[1] == 0x03 ? 2 : 0;
        if (raw.length < offset + 2) {
          return -1;
        }
        int pppProtocol = getShort(raw, offset);
        if (pppProtocol == 0x0021) {
          etherProtocol = PacketConstants.IPv4_TYPE;
        } else if (pppProtocol == 0x0057) {
          etherProtocol = PacketConstants.IPv6_TYPE;
        } else {
          return -1;
        }
        return offset + 2;
      default:
        return -1;
    }
  }

  /**
   * Finds the network layer of an unencrypted 802.11 data frame carrying
   * LLC/SNAP, and the source and destination addresses for its DS bits.
   */
  private int find80211NetworkLayer(int offset) {
    if (raw.length < offset + 24) {
      return -1;
    }
    int frameControl = raw[offset] & 0xff;
    int flags = raw[offset + 1] & 0xff;
    int type = (frameControl >> 2) & 0x3;
    int subtype = (frameControl >> 4) & 0xf;
    boolean toDs = (flags & 0x01) != 0;
    boolean fromDs = (flags & 0x02) != 0;
    boolean isProtected = (flags & 0x40) != 0;
    // Only data frames carry packets; subtypes 4-7 and 12-15 carry no payload
    if (type != 2 || (subtype & 0x4) != 0 || isProtected) {
      return -1;
    }
    // Address fields 1-3 start at 4, 10 and 16; address 4 at 24
    if (!toDs && !fromDs) {
      dstMacOffset = offset + 4;
      srcMacOffset = offset + 10;
    } else if (toDs && !fromDs) {
      srcMacOffset = offset + 10;
      dstMacOffset = offset + 16;
    } else if (!toDs) {
      dstMacOffset = offset + 4;
      srcMacOffset = offset + 16;
    } else {
      dstMacOffset = offset + 16;
      srcMacOffset = offset + 24;
    }
    int headerLength = toDs && fromDs ? 30 : 24;
    if ((subtype & 0x8) != 0) {
      // QoS control, plus HT control when the order bit is set
      headerLength += (flags & 0x80) != 0 ? 6 : 2;
    }
    int llc = offset + headerLength;
    // LLC/SNAP: AA AA 03 00 00 00, then the EtherType
    if (raw.length < llc + 8 || (raw[llc] & 0xff) != 0xaa || (raw[llc + 1] & 0xff) != 0xaa
        || raw[llc + 2] != 0x03) {
      return -1;
    }
    etherProtocol = getShort(raw, llc + 6);
    return llc + 8;
  }

  private int ipVersionProtocol(int offset) {
    if (raw.length <= offset) {
      return -1;
    }
    int version = (raw[offset] & 0xff) >>> 4;
    if (version == 4) {
      etherProtocol = PacketConstants.IPv4_TYPE;
    } else if (version == 6) {
      etherProtocol = PacketConstants.IPv6_TYPE;
    } else {
      return -1;
    }
    return offset;
  }

  @Override
  public String getEthernetSource() {
    return formatMac(srcMacOffset);
  }

  @Override
  public String getEthernetDestination() {
    return formatMac(dstMacOffset);
  }

  private String formatMac(int offset) {
    if (offset < 0 || offset + 6 > raw.length) {
      return null;
    }
    StringBuilder mac = new StringBuilder(17);
    for (int i = 0; i < 6; i++) {
      if (i > 0) {
        mac.append(':');
      }
      mac.append(String.format("%02X", raw[offset + i]));
    }
    return mac.toString();
  }

  public void setTimestamp(Instant timestamp) {
    setTimestampMicro(timestamp.getEpochSecond() * 1_000_000L + timestamp.getNano() / 1000);
  }

  @Override
  protected int getFrameEnd() {
    return raw.length;
  }

  @Override
  protected int processIpV6Packet() {
    try {
      return super.processIpV6Packet();
    } catch (IllegalStateException | ArrayIndexOutOfBoundsException e) {
      return -1;
    }
  }
}
