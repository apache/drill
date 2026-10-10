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
package org.apache.drill.exec.store.pcap.protocol;

import java.util.ArrayList;
import java.util.List;

/** Decoder context for one row: collects warnings, prefixed with the decoder's protocol. */
public class RowDecoderContext implements DecoderContext {
  private final boolean exposeCredentials;
  private final List<String> warnings = new ArrayList<>();
  private String protocol;
  private int mark;

  public RowDecoderContext(boolean exposeCredentials) {
    this.exposeCredentials = exposeCredentials;
  }

  @Override
  public boolean exposeCredentials() {
    return exposeCredentials;
  }

  @Override
  public void warn(String message) {
    warnings.add(protocol + ": " + message);
  }

  /** Starts a new row. */
  public void reset() {
    warnings.clear();
  }

  public List<String> warnings() {
    return warnings;
  }

  void begin(String protocol) {
    this.protocol = protocol;
    mark = warnings.size();
  }

  /** Drops warnings from a decoder that turned out not to handle the row. */
  void discard() {
    while (warnings.size() > mark) {
      warnings.remove(warnings.size() - 1);
    }
  }
}
