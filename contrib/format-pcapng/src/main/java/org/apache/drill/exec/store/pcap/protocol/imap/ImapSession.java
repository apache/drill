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
package org.apache.drill.exec.store.pcap.protocol.imap;

import org.apache.drill.exec.store.pcap.protocol.mail.MailCredentials;
import org.apache.drill.exec.store.pcap.protocol.mail.MailFields.Capped;
import org.apache.drill.exec.store.pcap.protocol.mail.MailMessage;

/** What was seen in one IMAP session. */
public class ImapSession {
  public String banner;
  public final Capped<String> capabilities = new Capped<>("capabilities");
  public final MailCredentials credentials = new MailCredentials();
  public boolean tlsStarted;
  public final Capped<String> selectedMailboxes = new Capped<>("selected_mailboxes");
  /** tag, command, argument */
  public final Capped<String[]> commands = new Capped<>("commands");
  public int commandCount;
  /** tag, status, text of tagged completions */
  public final Capped<String[]> responses = new Capped<>("responses");
  public final Capped<MailMessage> fetched = new Capped<>("fetched_messages");
}
