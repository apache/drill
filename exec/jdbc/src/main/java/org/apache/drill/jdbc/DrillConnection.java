/**
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

package org.apache.drill.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.drill.exec.client.DrillClient;


public interface DrillConnection extends Connection{

  // in java.sql.Connection from JDK 1.7, but declare here to allow other JDKs
  void setSchema(String schema) throws SQLException;

  // in java.sql.Connection from JDK 1.7, but declare here to allow other JDKs
  String getSchema() throws SQLException;

  /** Returns a view onto this connection's configuration properties. Code
   * within Optiq should use this view rather than calling
   * {@link java.util.Properties#getProperty(String)}. */
  DrillConnectionConfig config();

  public DrillClient getClient();
}
