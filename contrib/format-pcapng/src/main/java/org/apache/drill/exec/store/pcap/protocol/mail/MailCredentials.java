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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.store.pcap.protocol.DecoderContext;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * Credentials seen in a mail session: a plain login or a SASL exchange (PLAIN, LOGIN, CRAM-MD5).
 * The password is kept only when credentials may be exposed.
 */
public final class MailCredentials {
  public String mechanism;
  public String username;
  public boolean passwordPresent;
  public String password;
  private int saslStep;

  public static void define(SchemaBuilder f) {
    f.addNullable("auth_mechanism", MinorType.VARCHAR)
        .addNullable("username", MinorType.VARCHAR)
        .addNullable("password_present", MinorType.BIT)
        .addNullable("password", MinorType.VARCHAR);
  }

  public void write(TupleWriter t) {
    MailFields.setString(t, "auth_mechanism", mechanism);
    MailFields.setString(t, "username", username);
    t.scalar("password_present").setBoolean(passwordPresent);
    MailFields.setString(t, "password", password);
  }

  public void setUsername(String user) {
    username = MailLines.cap(user);
  }

  public void setPassword(String pass, DecoderContext context) {
    passwordPresent = true;
    if (context.exposeCredentials()) {
      password = MailLines.cap(pass);
    }
  }

  /** Starts a SASL exchange; the initial response may be null. */
  public void startSasl(String mech, String initialResponse, DecoderContext context) {
    mechanism = MailLines.cap(mech.toUpperCase(Locale.ROOT));
    saslStep = 0;
    if (initialResponse != null) {
      saslResponse(initialResponse, context);
    }
  }

  /** True while the mechanism expects more client responses (used when the server side is missing). */
  public boolean saslExpectsMore() {
    int expected;
    switch (mechanism == null ? "" : mechanism) {
      case "LOGIN":
        expected = 2;
        break;
      case "PLAIN":
      case "CRAM-MD5":
        expected = 1;
        break;
      default:
        expected = 0;
    }
    return saslStep < expected;
  }

  /** Handles one base64 client response of the current SASL exchange. */
  public void saslResponse(String line, DecoderContext context) {
    int step = saslStep++;
    if ("*".equals(line)) {
      return; // the client cancelled the exchange
    }
    byte[] decoded = base64(line);
    if (decoded == null) {
      context.warn("invalid base64 in " + mechanism + " response");
      return;
    }
    String text = new String(decoded, StandardCharsets.UTF_8);
    switch (mechanism) {
      case "PLAIN": {
        // authzid NUL authcid NUL passwd
        String[] parts = text.split("\0", -1);
        if (parts.length != 3) {
          context.warn("malformed PLAIN response");
          return;
        }
        setUsername(parts[1].isEmpty() ? parts[0] : parts[1]);
        setPassword(parts[2], context);
        break;
      }
      case "LOGIN":
        if (step == 0) {
          setUsername(text);
        } else if (step == 1) {
          setPassword(text, context);
        }
        break;
      case "CRAM-MD5": {
        // "user digest": the digest is not a password
        int space = text.lastIndexOf(' ');
        if (space > 0) {
          setUsername(text.substring(0, space));
          passwordPresent = true;
        }
        break;
      }
      default:
        break;
    }
  }

  /** Decodes base64 ("=" is an empty response), or returns null if it is not valid base64. */
  public static byte[] base64(String s) {
    if ("=".equals(s)) {
      return new byte[0];
    }
    try {
      return Base64.getDecoder().decode(s.trim());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
