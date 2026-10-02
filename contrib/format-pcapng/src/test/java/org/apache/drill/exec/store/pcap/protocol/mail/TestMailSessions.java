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

import java.nio.file.Paths;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.BeforeClass;
import org.junit.Test;

/** Query tests of the SMTP, POP3 and IMAP session decoders on the fixtures from mail_fixtures.py. */
public class TestMailSessions extends ClusterTest {

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));
    dirTestWatcher.copyResourceToRoot(Paths.get("decoders/"));
  }

  private static String table(String name, boolean expose) {
    return "table(dfs.`decoders/" + name + "/" + name + ".pcapng` (type => 'pcapng', sessionizeTCPStreams => true"
        + (expose ? ", exposeCredentials => true" : "") + ")) t";
  }

  @Test
  public void testSmtp() throws Exception {
    String sql = "select src_port, parsed_protocol, t.parsed_data.smtp.banner as banner, "
        + "t.parsed_data.smtp.helo as helo, t.parsed_data.smtp.username as username, "
        + "t.parsed_data.smtp.password as password, t.parsed_data.smtp.tls_started as tls, "
        + "t.parsed_data.smtp.rcpt_to[1] as rcpt, t.parsed_data.smtp.messages[0].subject as subject, "
        + "t.parsed_data.smtp.messages[0].size as size, t.parsed_data.smtp.commands[1].argument as auth, "
        + "decode_error from " + table("smtp", false) + " order by src_port";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_port", MinorType.INT)
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("banner", MinorType.VARCHAR)
        .addNullable("helo", MinorType.VARCHAR)
        .addNullable("username", MinorType.VARCHAR)
        .addNullable("password", MinorType.VARCHAR)
        .addNullable("tls", MinorType.BIT)
        .addNullable("rcpt", MinorType.VARCHAR)
        .addNullable("subject", MinorType.VARCHAR)
        .addNullable("size", MinorType.BIGINT)
        .addNullable("auth", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow(40001, "smtp", "mail.example.com ESMTP Postfix", "client.example.org", "alice", null, false,
            "carol@example.com", "Café", 199L, "PLAIN ***", null)
        .addRow(40002, "smtp", "mail.example.com ESMTP", "client.example.org", null, null, true,
            null, null, null, "", null)
        .addRow(40003, null, null, null, null, null, null, null, null, null, null, null)
        .addRow(40004, "smtp", "mail.example.com ESMTP", "client.example.org", null, null, false,
            null, "cut off", 36L, "FROM:<a@example.org>", "smtp: message data truncated at byte 83")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSmtpExposeCredentials() throws Exception {
    String sql = "select t.parsed_data.smtp.password as password, t.parsed_data.smtp.password_present as present "
        + "from " + table("smtp", true) + " where src_port = 40001";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("password", MinorType.VARCHAR)
        .addNullable("present", MinorType.BIT)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("secret", true)
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testPop3() throws Exception {
    String sql = "select src_port, parsed_protocol, t.parsed_data.pop3.banner as banner, "
        + "t.parsed_data.pop3.username as username, t.parsed_data.pop3.password_present as present, "
        + "t.parsed_data.pop3.tls_started as tls, t.parsed_data.pop3.message_count as message_count, "
        + "t.parsed_data.pop3.retrieved[0].subject as subject, t.parsed_data.pop3.retrieved[0].size as size, "
        + "t.parsed_data.pop3.commands[1].argument as pass, decode_error from " + table("pop3", false)
        + " order by src_port";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_port", MinorType.INT)
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("banner", MinorType.VARCHAR)
        .addNullable("username", MinorType.VARCHAR)
        .addNullable("present", MinorType.BIT)
        .addNullable("tls", MinorType.BIT)
        .addNullable("message_count", MinorType.INT)
        .addNullable("subject", MinorType.VARCHAR)
        .addNullable("size", MinorType.BIGINT)
        .addNullable("pass", MinorType.VARCHAR)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow(41001, "pop3", "POP3 server ready", "bob", true, false, 2, "Hi", 106L, "***", null)
        .addRow(41002, "pop3", "POP3 server ready", null, false, true, null, null, null, null, null)
        .addRow(41003, null, null, null, null, null, null, null, null, null, null)
        .addRow(41004, "pop3", "POP3 server ready", null, false, false, null, "cut off", 25L, null,
            "pop3: multi-line response truncated at byte 38 of server stream")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testImap() throws Exception {
    String sql = "select src_port, parsed_protocol, t.parsed_data.imap.banner as banner, "
        + "t.parsed_data.imap.capabilities[1] as capability, t.parsed_data.imap.username as username, "
        + "t.parsed_data.imap.tls_started as tls, t.parsed_data.imap.selected_mailboxes[0] as mailbox, "
        + "t.parsed_data.imap.commands[0].argument as login, "
        + "t.parsed_data.imap.fetched_messages[0].`from` as from0, "
        + "t.parsed_data.imap.fetched_messages[1].subject as subject1, "
        + "t.parsed_data.imap.fetched_messages[1].uid as uid1, decode_error from " + table("imap", false)
        + " order by src_port";
    RowSet results = client.queryBuilder().sql(sql).rowSet();
    TupleMetadata schema = new SchemaBuilder()
        .addNullable("src_port", MinorType.INT)
        .addNullable("parsed_protocol", MinorType.VARCHAR)
        .addNullable("banner", MinorType.VARCHAR)
        .addNullable("capability", MinorType.VARCHAR)
        .addNullable("username", MinorType.VARCHAR)
        .addNullable("tls", MinorType.BIT)
        .addNullable("mailbox", MinorType.VARCHAR)
        .addNullable("login", MinorType.VARCHAR)
        .addNullable("from0", MinorType.VARCHAR)
        .addNullable("subject1", MinorType.VARCHAR)
        .addNullable("uid1", MinorType.BIGINT)
        .addNullable("decode_error", MinorType.VARCHAR)
        .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow(42001, "imap", "[CAPABILITY IMAP4rev1 STARTTLS AUTH=PLAIN] Dovecot ready.", "IDLE", "bob", false,
            "INBOX", "bob ***", "Alice <alice@example.org>", "Héllo", 8L, null)
        .addRow(42002, "imap", "IMAP4rev1 ready", null, null, true, null, "", null, null, null, null)
        .addRow(42003, null, null, null, null, null, null, null, null, null, null, null)
        .addRow(42004, "imap", "IMAP4rev1 ready", null, null, false, null, "1 BODY[HEADER]", null, null, null,
            "imap: literal of 99999 bytes at byte 22 exceeds the server stream")
        .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }
}
