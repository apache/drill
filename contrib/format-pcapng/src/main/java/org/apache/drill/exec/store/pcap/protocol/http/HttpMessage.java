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
package org.apache.drill.exec.store.pcap.protocol.http;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.store.pcap.protocol.DecoderContext;

/** One HTTP/1.x request or response: start line and headers. */
public class HttpMessage {
  public boolean isRequest;
  public String method;
  public String uri;
  public String version;
  public Integer statusCode;
  public String reason;
  public final List<String[]> headers = new ArrayList<>();
  /** Offset just past the blank line ending the headers, or -1 if the headers are incomplete. */
  public int headerEnd = -1;
  public String username;
  public boolean passwordPresent;
  public String password;
  private DecoderContext context;
  // Content-Length is read once, so an invalid value is reported once
  private boolean contentLengthRead;
  private Long contentLength;

  void setContext(DecoderContext context) {
    this.context = context;
  }

  /** First header with this name, ignoring case; null if absent. */
  public String header(String name) {
    for (String[] h : headers) {
      if (h[0].equalsIgnoreCase(name)) {
        return h[1];
      }
    }
    return null;
  }

  /** Content-Length, or null if absent or invalid. */
  public Long contentLength() {
    if (!contentLengthRead) {
      contentLengthRead = true;
      contentLength = readContentLength();
    }
    return contentLength;
  }

  private Long readContentLength() {
    String value = header("Content-Length");
    if (value == null) {
      return null;
    }
    try {
      long length = Long.parseLong(value.trim());
      if (length >= 0) {
        return length;
      }
    } catch (NumberFormatException e) {
      // reported below
    }
    if (context != null) {
      context.warn("invalid Content-Length " + value);
    }
    return null;
  }

  public boolean isChunked() {
    String value = header("Transfer-Encoding");
    return value != null && value.toLowerCase().contains("chunked");
  }
}
