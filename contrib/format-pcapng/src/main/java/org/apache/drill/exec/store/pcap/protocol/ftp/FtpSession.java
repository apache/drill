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
package org.apache.drill.exec.store.pcap.protocol.ftp;

import java.util.ArrayList;
import java.util.List;

/** What the control channel of one FTP session revealed. */
public class FtpSession {
  public String banner;
  public String username;
  public boolean passwordPresent;
  public String password;
  public boolean tlsStarted;
  public String systemType;
  public final List<String> currentDirectories = new ArrayList<>();
  public final List<Transfer> transfers = new ArrayList<>();
  public final List<Command> commands = new ArrayList<>();
  public final List<Reply> replies = new ArrayList<>();

  public static class Command {
    public String command;
    public String argument;
  }

  public static class Reply {
    public int code;
    public String text;
  }

  public static class Transfer {
    public String command;
    public String path;
    public Integer replyCode;
    public String dataAddress;
  }
}
