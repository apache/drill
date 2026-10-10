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
package org.apache.drill.exec.store.pcap.protocol.dns;

import java.util.ArrayList;
import java.util.List;

/** A parsed DNS message. */
public class DnsMessage {
  public int transactionId;
  public boolean isResponse;
  public int opcode;
  public int rcode;
  public boolean authoritative;
  public boolean truncated;
  public boolean recursionDesired;
  public boolean recursionAvailable;
  public final List<Question> questions = new ArrayList<>();
  public final List<Record> answers = new ArrayList<>();
  public final List<Record> authorities = new ArrayList<>();
  public final List<Record> additionals = new ArrayList<>();

  public static class Question {
    public String name;
    public String type;
    public int dnsClass;
  }

  public static class Record {
    public String name;
    public String type;
    public int dnsClass;
    public long ttl;
    public String data;
  }
}
