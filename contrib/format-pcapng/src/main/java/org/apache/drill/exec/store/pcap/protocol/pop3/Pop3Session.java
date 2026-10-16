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
package org.apache.drill.exec.store.pcap.protocol.pop3;

import org.apache.drill.exec.store.pcap.protocol.mail.MailCredentials;
import org.apache.drill.exec.store.pcap.protocol.mail.MailFields.Capped;
import org.apache.drill.exec.store.pcap.protocol.mail.MailMessage;

/** What was seen in one POP3 session. */
public class Pop3Session {
  public String banner;
  public final Capped<String> capabilities = new Capped<>("capabilities");
  public final MailCredentials credentials = new MailCredentials();
  public boolean tlsStarted;
  public Integer messageCount;
  public final Capped<MailMessage> retrieved = new Capped<>("retrieved");
  /** command, argument */
  public final Capped<String[]> commands = new Capped<>("commands");
  public int commandCount;
  /** status (+OK, -ERR or + for a continuation), text */
  public final Capped<String[]> replies = new Capped<>("replies");
}
