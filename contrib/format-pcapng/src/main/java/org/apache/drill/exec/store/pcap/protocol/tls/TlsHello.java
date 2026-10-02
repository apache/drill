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
package org.apache.drill.exec.store.pcap.protocol.tls;

import java.util.ArrayList;
import java.util.List;

/** A parsed TLS ClientHello or ServerHello. Fields not present in the message are null. */
public class TlsHello {
  /** client_hello or server_hello. */
  public String handshakeType;
  public String recordVersion;
  /** The legacy_version field of the hello. */
  public String version;
  /** From the supported_versions extension; the selected version for a ServerHello. */
  public List<String> supportedVersions;
  public String sessionId;
  public String sni;
  public List<String> alpn;
  /** ClientHello only. */
  public List<Integer> cipherSuites;
  /** ServerHello only. */
  public Integer cipherSuite;
  public final List<Integer> extensions = new ArrayList<>();
  public List<Integer> supportedGroups;
  public List<Integer> ecPointFormats;
  public List<Integer> signatureAlgorithms;
  public String ja3;
  public String ja3Hash;
  public String ja3s;
  public String ja3sHash;
}
