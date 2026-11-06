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
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/** Decoder context for mail parser unit tests: records warnings. */
public final class MailTestContext implements DecoderContext {
  public final boolean expose;
  public final List<String> warnings = new ArrayList<>();

  public MailTestContext(boolean expose) {
    this.expose = expose;
  }

  @Override
  public boolean exposeCredentials() {
    return expose;
  }

  @Override
  public void warn(String message) {
    warnings.add(message);
  }

  /** CRLF-joined lines, each terminated by CRLF. */
  public static byte[] lines(String... lines) {
    StringBuilder b = new StringBuilder();
    for (String line : lines) {
      b.append(line).append("\r\n");
    }
    return b.toString().getBytes(StandardCharsets.UTF_8);
  }
}
