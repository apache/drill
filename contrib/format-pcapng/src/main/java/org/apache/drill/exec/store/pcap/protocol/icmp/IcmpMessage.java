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
package org.apache.drill.exec.store.pcap.protocol.icmp;

/** The fields of one ICMP or ICMPv6 message. Fields that do not apply are null. */
public class IcmpMessage {
  /** 4 for ICMP, 6 for ICMPv6. */
  public int version;
  public int type;
  public int code;
  public String typeName;
  public String codeName;
  public Integer identifier;
  public Integer sequence;
  public Integer mtu;
  public String gateway;
  public String targetAddress;
  public String destinationAddress;
  public String originalSrcIp;
  public String originalDstIp;
  public Integer originalProtocol;
  public Integer originalSrcPort;
  public Integer originalDstPort;
}
