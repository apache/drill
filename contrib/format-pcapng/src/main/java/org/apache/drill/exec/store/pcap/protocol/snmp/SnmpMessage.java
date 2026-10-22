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
package org.apache.drill.exec.store.pcap.protocol.snmp;

import java.util.ArrayList;
import java.util.List;

/** A parsed SNMP message (v1, v2c or v3). */
public class SnmpMessage {
  public String version;
  public boolean communityPresent;
  /** Set only when credentials are exposed. */
  public String community;
  public String pduType;
  public Long requestId;
  public Integer errorStatus;
  public Integer errorIndex;
  public Integer nonRepeaters;
  public Integer maxRepetitions;
  public String enterprise;
  public String agentAddress;
  public Integer genericTrap;
  public Long specificTrap;
  public Long timeStamp;
  public Long msgId;
  public String msgUserName;
  public String securityLevel;
  public String engineId;
  public boolean encrypted;
  public final List<Varbind> varbinds = new ArrayList<>();

  public static class Varbind {
    public String oid;
    public String valueType;
    public String value;
  }
}
