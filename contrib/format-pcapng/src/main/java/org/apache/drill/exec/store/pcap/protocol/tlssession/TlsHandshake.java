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
package org.apache.drill.exec.store.pcap.protocol.tlssession;

import java.util.ArrayList;
import java.util.List;

/** The cleartext part of a TLS handshake seen in one TCP session. Null fields were not seen. */
public class TlsHandshake {
  // From the ClientHello
  boolean clientHelloSeen;
  String clientVersion;
  final List<String> clientSupportedVersions = new ArrayList<>();
  String sni;
  final List<String> alpnOffered = new ArrayList<>();
  byte[] clientSessionId;

  // From the ServerHello
  int selectedVersion = -1;
  String serverVersion;
  String alpnSelected;
  Integer cipherSuite;
  String cipherSuiteName;
  byte[] serverSessionId;
  boolean pskAccepted;
  Boolean helloRetryRequest;

  // Derived when both directions have been read
  Boolean sessionResumed;
  Boolean certificateEncrypted;

  // From the Certificate message (TLS 1.2 and earlier)
  Integer certificateCount;
  final List<TlsCertificate> certificates = new ArrayList<>();
}
