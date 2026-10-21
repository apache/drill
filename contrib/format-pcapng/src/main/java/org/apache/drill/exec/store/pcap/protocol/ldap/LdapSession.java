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
package org.apache.drill.exec.store.pcap.protocol.ldap;

import java.util.ArrayList;
import java.util.List;

/** The cleartext metadata of one LDAP session (RFC 4511): the bind, searches and returned entries. */
public class LdapSession {
  public Integer version;
  public String bindDn;
  public String authType;
  public boolean passwordPresent;
  /** Set only when credentials are exposed. */
  public String password;
  public Integer bindResultCode;
  public String bindResult;
  public final List<Search> searches = new ArrayList<>();
  public final List<String> entriesReturned = new ArrayList<>();
  public int operationCount;

  public static class Search {
    public String baseDn;
    public String scope;
    public String filter;
  }
}
