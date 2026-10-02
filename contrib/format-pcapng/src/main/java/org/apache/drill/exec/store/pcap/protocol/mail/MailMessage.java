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
package org.apache.drill.exec.store.pcap.protocol.mail;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.MapBuilder;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/** A message transferred or retrieved in a mail session: its main headers and size. */
public final class MailMessage {
  public MailHeaders headers = new MailHeaders();
  /** POP3 and IMAP message sequence number. */
  public Integer number;
  /** IMAP UID. */
  public Long uid;
  /** Size in bytes. */
  public Long size;

  /** Defines the header fields and size. */
  public static void define(MapBuilder m) {
    m.addNullable("from", MinorType.VARCHAR)
        .addNullable("to", MinorType.VARCHAR)
        .addNullable("cc", MinorType.VARCHAR)
        .addNullable("subject", MinorType.VARCHAR)
        .addNullable("date", MinorType.VARCHAR)
        .addNullable("message_id", MinorType.VARCHAR)
        .addNullable("size", MinorType.BIGINT);
  }

  public void write(TupleWriter t) {
    MailFields.setInt(t, "number", number);
    MailFields.setLong(t, "uid", uid);
    MailFields.setString(t, "from", headers.from);
    MailFields.setString(t, "to", headers.to);
    MailFields.setString(t, "cc", headers.cc);
    MailFields.setString(t, "subject", headers.subject);
    MailFields.setString(t, "date", headers.date);
    MailFields.setString(t, "message_id", headers.messageId);
    MailFields.setLong(t, "size", size);
  }
}
