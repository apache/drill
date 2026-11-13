/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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
package org.apache.drill.exec.security.spi;

import java.util.Set;

/**
 * Drill access authorization SPI interface.
 *
 * <p>Mirrors Presto's {@code SystemAccessControl}: the engine (via
 * {@code AccessAuthorizerManager}) discovers an {@link AccessAuthorizerFactory}
 * through {@code ServiceLoader} and calls {@code factory.createAuthorizer(config)} to
 * obtain a fully-initialized instance. Implementations must complete all
 * initialization in the factory/constructor phase; there is no separate
 * {@code init()} lifecycle method.</p>
 *
 * <p>This interface and its parameter types ({@link UserIdentity}, strings,
 * sets) depend only on the JDK, so implementations can live outside the
 * Drill engine (e.g. an Apache Ranger plugin) with no dependency beyond
 * this small SPI module — analogous to Presto's {@code presto-spi}.</p>
 *
 * <p>There is deliberately no {@code isEnabled()} method: an instance that
 * exists is initialized and active (the factory completes initialization or
 * throws, fail-closed). Whether authorization is enabled at all is an
 * engine-side configuration concern — the engine selects this SPI or its own
 * allow-all implementation (mirroring Presto's
 * {@code SystemAccessControl} / {@code AllowAllAccessControl}).</p>
 *
 * <p>Access types are identified by the {@link AccessType} enum (mirroring
 * Presto's {@code Privilege}). Callers pass the constant to
 * {@link #checkTableAccess} or {@link #checkColumnAccess}. This avoids a
 * dedicated method per access type and keeps the interface stable as new
 * operations are added.</p>
 */
public interface AccessAuthorizer {

  /**
   * Checks table-level access permission.
   *
   * @param user       the querying user identity
   * @param dataSource the data source name (StoragePlugin name, e.g. "dfs")
   * @param schema     the schema path (e.g. "dfs.tmp")
   * @param table      the table name
   * @param accessType the access type (e.g. {@link AccessType#SELECT},
   *                   {@link AccessType#CREATE})
   * @return {@code true} if access is allowed
   */
  boolean checkTableAccess(UserIdentity user, String dataSource, String schema,
                           String table, AccessType accessType);

  /**
   * Checks column-level access permission for a set of columns. Returns
   * {@code true} only if the user has the specified access type on ALL given
   * columns.
   *
   * @param user       the querying user identity
   * @param dataSource the data source name (StoragePlugin name, e.g. "dfs")
   * @param schema     the schema path (e.g. "dfs.tmp")
   * @param table      the table name
   * @param columns    the set of column names being accessed
   * @param accessType the access type (e.g. {@link AccessType#SELECT})
   * @return {@code true} if access is allowed for every column
   */
  boolean checkColumnAccess(UserIdentity user, String dataSource, String schema,
                            String table, Set<String> columns, AccessType accessType);

  /**
   * Releases any resources held by this authorizer (background policy-refresh
   * threads, caches, open connections). Invoked by the engine when the
   * Drillbit shuts down; after this call the instance must be considered
   * unusable, and the engine drops its cached reference so a subsequent
   * Drillbit start initializes a fresh instance.
   *
   * <p>Default no-op: implementations that hold no resources do not need to
   * override. Implementations should be idempotent and must not throw checked
   * exceptions — the engine logs and ignores close failures rather than
   * aborting shutdown.</p>
   */
  default void close() {
    // no-op by default
  }
}
