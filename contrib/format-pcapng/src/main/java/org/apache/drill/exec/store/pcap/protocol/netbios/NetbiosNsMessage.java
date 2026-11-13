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
package org.apache.drill.exec.store.pcap.protocol.netbios;

import java.util.ArrayList;
import java.util.List;

/** A parsed NetBIOS Name Service message (RFC 1002). */
public class NetbiosNsMessage {
  public int transactionId;
  public boolean isResponse;
  public String opcode;
  public int rcode;
  public boolean broadcast;
  public final List<Question> questions = new ArrayList<>();
  public final List<Record> answers = new ArrayList<>();
  public final List<Record> authorities = new ArrayList<>();
  public final List<Record> additionals = new ArrayList<>();

  public static class Question {
    public String name;
    public int suffix;
    public String suffixName;
    public String type;
  }

  public static class Record {
    public String name;
    public int suffix;
    public String suffixName;
    public String type;
    public long ttl;
    /** Addresses of NB and A records; null for other types. */
    public List<String> addresses;
    /** B, P, M or H, from the first NB entry. */
    public String nodeType;
    public Boolean group;
    /** Names of an NBSTAT (node status) response, as NAME&lt;xx&gt;. */
    public List<String> names;
    public String macAddress;
  }
}
