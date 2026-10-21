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
package org.apache.drill.exec.store.pcap.protocol.ntlm;

/**
 * Identity metadata extracted from one NTLMSSP message (MS-NLMP). Only names, message type and the
 * negotiate flags are kept: challenges, responses, session keys and anything derived from them (such as
 * a crackable NetNTLM hash line) are never stored or exposed, regardless of the exposeCredentials option.
 */
public class NtlmMessage {

  public static final int NEGOTIATE = 1;
  public static final int CHALLENGE = 2;
  public static final int AUTHENTICATE = 3;

  /** 1 = NEGOTIATE, 2 = CHALLENGE, 3 = AUTHENTICATE. */
  public int messageType;

  /** The NegotiateFlags field, present in all three message types. */
  public long flags;

  // CHALLENGE (type 2)
  public String targetName;
  public String targetNetbiosComputer;
  public String targetNetbiosDomain;
  public String targetDnsComputer;
  public String targetDnsDomain;

  // AUTHENTICATE (type 3)
  public String userName;
  public String domainName;
  public String workstation;
  /** "NTLMv1", "NTLMv2", or null when the NT response is absent or an unexpected length. */
  public String ntlmVersion;
}
