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
package org.apache.drill.exec.store.pcap.protocol.ssh;

import java.util.List;

/** The cleartext start of an SSH connection: both identification strings and KEXINIT messages. */
public class SshSession {
  public Side client;
  public Side server;

  /** What one endpoint sent. Lists are null when its KEXINIT was not read. */
  public static class Side {
    public String version;
    public String software;
    public String comments;
    public List<String> kexAlgorithms;
    public List<String> hostKeyAlgorithms;
    /** Encryption, MAC and compression lists for this endpoint's sending direction. */
    public List<String> ciphers;
    public List<String> macs;
    public List<String> compression;
    public String hasshString;
    public String hassh;
  }
}
