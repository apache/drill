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
package org.apache.drill.exec.store.pcap.protocol.quic;

import java.util.List;

/** A parsed QUIC long-header packet. The ClientHello fields are filled only for a decrypted v1 Initial. */
public class QuicInitial {
  /** initial, 0rtt, handshake, retry or version_negotiation. */
  public String packetType;
  /** The 32-bit version, as eight hex digits. */
  public String version;
  public String dcid;
  public String scid;
  public String sni;
  public List<String> alpn;
  public List<String> supportedVersions;
  public List<Integer> cipherSuites;
  public String ja4;
}
