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
package org.apache.drill.exec.store.phoenix;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.queryserver.server.QueryServer;

/** Wraps up the query server for tests. */
public class QueryServerThread extends Thread {

  private final QueryServer main;

  public QueryServerThread(String[] argv, Configuration conf) {
    this(argv, conf, null);
  }

  public QueryServerThread(String[] argv, Configuration conf, String name) {
    this(new QueryServer(argv, conf), name);
  }

  private QueryServerThread(QueryServer m, String name) {
    super(m, "query server" + (name == null ? "" : (" - " + name)));
    this.main = m;
    setDaemon(true);
  }

  public QueryServer getQueryServer() {
    return main;
  }
}
