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
package org.apache.drill.exec.store.pcap.protocol.smb;

import java.util.ArrayList;
import java.util.List;

/** What the cleartext part of an SMB2/SMB3 session reveals: the negotiation and the authenticating identity. */
public class SmbSession {

  /** Negotiated dialect, such as {@code 3.1.1}, or {@code SMB1} for a session that never left SMB1. */
  public String dialect;

  /** Dialects the client offered in its NEGOTIATE request. */
  public final List<String> clientDialects = new ArrayList<>();

  public Boolean signingRequired;

  /** True once an SMB3 transform (encrypted) header was seen; the rest of that direction is opaque. */
  public boolean encryption;

  public String serverGuid;
  public String clientGuid;

  /** {@code ntlmssp} or {@code kerberos}, from the SESSION_SETUP security blob. */
  public String authType;

  public String userName;
  public String domainName;
  public String workstation;
  /** {@code NTLMv1} or {@code NTLMv2}, from the NTLM AUTHENTICATE message. */
  public String ntlmVersion;
}
