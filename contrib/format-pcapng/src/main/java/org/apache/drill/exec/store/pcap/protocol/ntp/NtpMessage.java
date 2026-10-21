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
package org.apache.drill.exec.store.pcap.protocol.ntp;

import java.time.Instant;

/** One NTP message. Fields that the message's mode does not carry are null. */
public class NtpMessage {
  public Integer leapIndicator;
  public int version;
  public String mode;
  public Integer stratum;
  public Integer poll;
  public Integer precision;
  public Double rootDelay;
  public Double rootDispersion;
  public String referenceId;
  public Instant referenceTime;
  public Instant originTime;
  public Instant receiveTime;
  public Instant transmitTime;
  /** Opcode of a control (mode 6) message or request code of a private (mode 7) message. */
  public Integer requestCode;
}
