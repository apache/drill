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

/** The DNS messages of one TCP session. */
public class DnsSession {
  public final List<Query> queries = new ArrayList<>();
  public final List<Answer> answers = new ArrayList<>();
  public int clientMessages;
  public int serverMessages;
  public boolean isZoneTransfer;

  public static class Query {
    public int transactionId;
    public String name;
    public String type;
  }

  public static class Answer {
    public int transactionId;
    public String name;
    public String type;
    public long ttl;
    public String data;
  }
}
