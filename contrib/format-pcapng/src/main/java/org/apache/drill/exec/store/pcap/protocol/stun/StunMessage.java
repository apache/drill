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
package org.apache.drill.exec.store.pcap.protocol.stun;

import java.util.ArrayList;
import java.util.List;

/** A parsed STUN message. Attributes not present in the message are null. */
public class StunMessage {
  /** request, indication, success_response or error_response. */
  public String messageClass;
  /** binding, allocate, ... or the method number. */
  public String messageMethod;
  public String transactionId;
  public String xorMappedAddress;
  public String mappedAddress;
  public String software;
  public String realm;
  public String nonce;
  public Integer errorCode;
  public String errorReason;
  public String username;
  /** Type and length of each attribute, in order. */
  public final List<int[]> attributes = new ArrayList<>();
}
