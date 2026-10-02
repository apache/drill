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
package org.apache.drill.exec.store.pcap.protocol.smtp;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.mail.MailCredentials;
import org.apache.drill.exec.store.pcap.protocol.mail.MailFields.Capped;
import org.apache.drill.exec.store.pcap.protocol.mail.MailMessage;

/** What was seen in one SMTP session. */
public class SmtpSession {

  /** One reply, possibly multi-line. */
  public static class Reply {
    public final Integer code;
    public final List<String> lines = new ArrayList<>();
    public String text;

    Reply(int code) {
      this.code = code;
    }
  }

  public String banner;
  public String helo;
  public final Capped<String> extensions = new Capped<>("extensions");
  public final MailCredentials credentials = new MailCredentials();
  public boolean tlsStarted;
  public String mailFrom;
  public final Capped<String> rcptTo = new Capped<>("rcpt_to");
  public final Capped<MailMessage> messages = new Capped<>("messages");
  public int messageCount;
  /** command, argument */
  public final Capped<String[]> commands = new Capped<>("commands");
  public int commandCount;
  public final Capped<Reply> replies = new Capped<>("replies");
}
