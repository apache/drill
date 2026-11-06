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
package org.apache.drill.exec.store.pcap.protocol.syslog;

/** One syslog message. Fields the message does not carry are null. */
public class SyslogMessage {
  public int facility;
  public String facilityName;
  public int severity;
  public String severityName;
  /** Protocol version of an RFC 5424 message; null for RFC 3164 and bare messages. */
  public Integer version;
  public String timestampText;
  public String hostname;
  public String appName;
  public String procId;
  public String msgId;
  public String structuredData;
  public String message;
}
