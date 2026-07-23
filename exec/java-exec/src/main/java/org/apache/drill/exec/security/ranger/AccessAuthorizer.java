/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.security.ranger;

import java.util.Set;

/**
 * Drill access authorization SPI interface.
 *
 * <p>The {@code exec/java-exec} module only depends on this interface; concrete
 * implementations (such as the Ranger plugin in {@code drill-ranger}) are loaded
 * reflectively via {@link AccessAuthorizerFactory} based on configuration.
 */
public interface AccessAuthorizer {

  /**
   * Initializes the authorizer. Called once at Drillbit startup.
   *
   * @param serviceName the authorization service name (e.g. Ranger service instance name)
   */
  void init(String serviceName);

  /**
   * @return {@code true} if the authorizer is enabled and successfully initialized
   */
  boolean isEnabled();

  /**
   * Checks table-level access permission.
   *
   * @param user       the querying user name
   * @param dataSource the data source name (StoragePlugin name, e.g. "dfs")
   * @param schema     the schema path (e.g. "dfs.tmp")
   * @param table      the table name
   * @param accessType the access type string ("SELECT", "CREATE", "INSERT", etc.)
   * @return {@code true} if access is allowed (fail-open returns {@code true} when disabled)
   */
  boolean checkTableAccess(String user, String dataSource, String schema,
                           String table, String accessType);

  /**
   * Checks column-level access permission for a set of columns. Returns {@code true} only
   * if the user has the specified access type on ALL given columns.
   *
   * @param user       the querying user name
   * @param dataSource the data source name (StoragePlugin name, e.g. "dfs")
   * @param schema     the schema path (e.g. "dfs.tmp")
   * @param table      the table name
   * @param columns    the set of column names being accessed
   * @param accessType the access type string ("SELECT", etc.)
   * @return {@code true} if access is allowed for every column (fail-open returns {@code true} when disabled)
   */
  boolean checkColumnAccess(String user, String dataSource, String schema,
                            String table, Set<String> columns, String accessType);
}
