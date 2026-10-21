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
package org.apache.drill.exec.server.rest.workflow;

import jakarta.mail.Session;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import org.apache.drill.exec.server.rest.smtp.SmtpConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SmtpMailerTest {

  private static SmtpConfig cfg(String encryption) {
    SmtpConfig c = new SmtpConfig();
    c.setHost("mail.example.com");
    c.setPort(587);
    c.setUsername("u");
    c.setPassword("p");
    c.setFromAddress("drill@example.com");
    c.setFromName("Apache Drill");
    c.setEncryption(encryption);
    c.setEnabled(true);
    return c;
  }

  @Test
  public void propertiesPerEncryption() {
    Properties tls = SmtpMailer.properties(cfg("starttls"));
    assertEquals("true", tls.getProperty("mail.smtp.starttls.enable"));
    assertEquals("true", tls.getProperty("mail.smtp.auth"));
    assertEquals("true", SmtpMailer.properties(cfg("ssl")).getProperty("mail.smtp.ssl.enable"));
    assertNull(SmtpMailer.properties(cfg("none")).getProperty("mail.smtp.starttls.enable"));
  }

  @Test
  public void unknownEncryptionIsRejectedInsteadOfSendingPlaintext() {
    assertThrows(IllegalStateException.class, () -> SmtpMailer.properties(cfg("tls")));
  }

  @Test
  public void messageHeaders() throws Exception {
    MimeMessage m = SmtpMailer.message(Session.getInstance(new Properties()), cfg("starttls"),
        List.of("a@example.com", "b@example.com"), "Subj", "Body");
    assertEquals("Subj", m.getSubject());
    assertEquals(2, m.getAllRecipients().length);
    assertEquals("Apache Drill", ((InternetAddress) m.getFrom()[0]).getPersonal());
    assertEquals("Body", m.getContent());
  }
}
