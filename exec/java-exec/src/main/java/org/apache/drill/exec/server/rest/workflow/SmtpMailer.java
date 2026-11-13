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

import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;
import org.apache.drill.exec.server.rest.smtp.SmtpConfig;

import java.util.List;
import java.util.Properties;

/** Sends plain-text mail through the admin-configured SMTP server. */
public final class SmtpMailer {

  private SmtpMailer() { }

  public static void send(SmtpConfig cfg, List<String> to, String subject, String body) throws Exception {
    if (cfg == null || !cfg.isEnabled() || cfg.getHost() == null || cfg.getHost().isEmpty()) {
      throw new IllegalStateException("SMTP is not configured; an admin can set it up under SMTP settings");
    }
    boolean auth = cfg.getUsername() != null && !cfg.getUsername().isEmpty();
    Session session = Session.getInstance(properties(cfg), auth ? new Authenticator() {
      @Override
      protected PasswordAuthentication getPasswordAuthentication() {
        return new PasswordAuthentication(cfg.getUsername(), cfg.getPassword());
      }
    } : null);
    Transport.send(message(session, cfg, to, subject, body));
  }

  static Properties properties(SmtpConfig cfg) {
    Properties p = new Properties();
    p.put("mail.smtp.host", cfg.getHost());
    p.put("mail.smtp.port", String.valueOf(cfg.getPort()));
    p.put("mail.smtp.connectiontimeout", "15000");
    p.put("mail.smtp.timeout", "30000");
    if (cfg.getUsername() != null && !cfg.getUsername().isEmpty()) {
      p.put("mail.smtp.auth", "true");
    }
    String enc = cfg.getEncryption() == null ? "starttls" : cfg.getEncryption();
    if (enc.equalsIgnoreCase("starttls")) {
      p.put("mail.smtp.starttls.enable", "true");
      p.put("mail.smtp.starttls.required", "true");
    } else if (enc.equalsIgnoreCase("ssl")) {
      p.put("mail.smtp.ssl.enable", "true");
    } else if (!enc.equalsIgnoreCase("none")) {
      throw new IllegalStateException("Unknown SMTP encryption '" + enc + "'");
    }
    return p;
  }

  static MimeMessage message(Session session, SmtpConfig cfg, List<String> to, String subject, String body)
      throws Exception {
    MimeMessage m = new MimeMessage(session);
    m.setFrom(new InternetAddress(cfg.getFromAddress(), cfg.getFromName()));
    for (String addr : to) {
      m.addRecipient(Message.RecipientType.TO, new InternetAddress(addr, true));
    }
    m.setSubject(subject, "UTF-8");
    m.setText(body, "UTF-8");
    return m;
  }
}
