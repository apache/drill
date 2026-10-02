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
package org.apache.drill.exec.store.pcap.protocol.ssdp;

import java.util.ArrayList;
import java.util.List;

/** One SSDP message: an M-SEARCH or NOTIFY request, or a search response. */
public class SsdpMessage {
  /** M-SEARCH or NOTIFY; null for a response. */
  public String method;
  /** Status code of a response; null for a request. */
  public Integer statusCode;
  public String st;
  public String nt;
  public String nts;
  public String usn;
  public String location;
  public String server;
  public String userAgent;
  public String man;
  public Integer mx;
  public String cacheControl;
  /** Name and value of each header, in order, up to the cap. */
  public final List<String[]> headers = new ArrayList<>();
}
