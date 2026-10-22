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
package org.apache.drill.exec.store.pcap.protocol.radius;

import java.util.ArrayList;
import java.util.List;

/** A parsed RADIUS packet (RFC 2865, RFC 2866). */
public class RadiusMessage {
  public int code;
  public String codeName;
  public int identifier;
  public String authenticator;
  public String username;
  public boolean passwordPresent;
  public String nasIpAddress;
  public String nasIdentifier;
  public Long nasPort;
  public String callingStationId;
  public String calledStationId;
  public String framedIpAddress;
  public String acctStatusType;
  public String acctSessionId;
  public String replyMessage;
  public final List<Attribute> attributes = new ArrayList<>();

  public static class Attribute {
    public int type;
    /** Hex value; null for a password attribute unless credentials are exposed. */
    public String value;
  }
}
