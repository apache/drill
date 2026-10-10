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
package org.apache.drill.exec.store.pcap.protocol.sip;

import java.util.ArrayList;
import java.util.List;

/** One SIP request or response. Absent headers are null. */
public class SipMessage {
  public boolean isRequest;
  public String method;
  public String requestUri;
  public Integer statusCode;
  public String reason;
  public String from;
  public String to;
  public String callId;
  public String cseq;
  public String userAgent;
  public String contact;
  /** Every Via value, from all Via headers, in order. */
  public final List<String> via = new ArrayList<>();
  public String contentType;
  public Long contentLength;
  /** Username of a Digest Authorization or Proxy-Authorization header. */
  public String username;
  /** Always false: SIP Digest never carries the password itself. */
  public boolean passwordPresent;
  /** Name (as sent) and value of each header, in order, up to the cap. */
  public final List<String[]> headers = new ArrayList<>();
}
