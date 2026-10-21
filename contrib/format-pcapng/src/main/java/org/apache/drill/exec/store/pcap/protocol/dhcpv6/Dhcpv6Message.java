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
package org.apache.drill.exec.store.pcap.protocol.dhcpv6;

import java.util.ArrayList;
import java.util.List;

/** The parsed fields of one DHCPv6 message. For a relay message, most fields come from the relayed message. */
public class Dhcpv6Message {
  String messageType;
  String relayedMessageType;
  Integer hopCount;
  String linkAddress;
  String peerAddress;
  Integer transactionId;
  String clientDuid;
  String serverDuid;
  Integer statusCode;
  String statusMessage;
  String fqdn;
  final List<String> iaAddresses = new ArrayList<>();
  final List<String> iaPrefixes = new ArrayList<>();
  final List<String> dnsServers = new ArrayList<>();
  final List<String> domainList = new ArrayList<>();
  final List<Integer> optionRequestList = new ArrayList<>();
  final List<Option> options = new ArrayList<>();

  /** One top-level option of the (relayed) client or server message. */
  static class Option {
    final int code;
    final String value;

    Option(int code, String value) {
      this.code = code;
      this.value = value;
    }
  }
}
