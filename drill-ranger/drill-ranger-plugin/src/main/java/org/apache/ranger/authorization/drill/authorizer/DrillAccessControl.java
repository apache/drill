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
package org.apache.ranger.authorization.drill.authorizer;


import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.drill.resource.DrillAccessType;
import org.apache.ranger.authorization.drill.resource.DrillResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Drill-facing authorization facade and single entry point for all Ranger
 * access checks from Drill core hook points.
 *
 * <p>This class is designed to minimize the code changes required in Drill core.
 * Each hook point is a single static method call:</p>
 *
 * <pre>{@code
 * if (!DrillAccessControl.checkTableAccess(userName, storageEngineName, schemaPath, tableName)) {
 *   throw UserException.permissionError()
 *       .message("Access denied for user %s on table %s.%s", userName, schemaPath, tableName)
 *       .build();
 * }
 * }</pre>
 *
 * <p>The class is initialized once at Drillbit startup via {@link #init(String)}.
 * Until initialized, {@link #isEnabled()} returns {@code false} and all checks
 * pass (fail-open), so the plugin is non-intrusive when disabled.</p>
 *
 * <p>The service name is read from the configuration property
 * {@code ranger.plugin.drill.service.name} (defined in ranger-drill-security.xml),
 * but can also be passed directly to {@link #init(String)} for flexibility.</p>
 */
public class DrillAccessControl {

  private static final Logger LOG = LoggerFactory.getLogger(DrillAccessControl.class);

  private static volatile boolean enabled = false;
  private static volatile DrillAuthorizer authorizer;

  // Set of system schemas that bypass authorization (information_schema, sys, etc.)
  // Stored in uppercase; isSystemSchema() uppercases input before lookup so the
  // bypass is case-insensitive (e.g. "information_schema", "INFORMATION_SCHEMA",
  // "Sys", "SYS" all match).
  // TODO: Currently system schemas bypass authorization entirely. Future work:
  //   - Implement fine-grained system table access control (e.g. restrict
  //     INFORMATION_SCHEMA columns, or allow only SYS schema metadata queries).
  //   - Make SYSTEM_SCHEMAS configurable via drill-override.conf or Ranger
  //     service-def options, so deployments can extend or override the list.
  //   - Consider per-user system schema visibility (e.g. hide sys.memory_state
  //     from non-admin users) instead of an all-or-nothing bypass.
  private static final Set<String> SYSTEM_SCHEMAS = new HashSet<>(Arrays.asList(
      "INFORMATION_SCHEMA", "SYS"
  ));

  private DrillAccessControl() {
  }

  /**
   * Initializes the Ranger Drill plugin. Called once at Drillbit startup.
   *
   * @param serviceName the Ranger service instance name (must match a service created in Ranger Admin)
   */
  public static synchronized void init(String serviceName) {
    if (authorizer != null) {
      return;
    }
    try {
      LOG.info("Initializing Ranger Drill authorization plugin for service: {}", serviceName);
      authorizer = new DrillAuthorizer(serviceName);
      enabled = true;
      LOG.info("Ranger Drill authorization plugin initialized successfully");
    } catch (Exception e) {
      LOG.error("Failed to initialize Ranger Drill plugin — authorization DISABLED", e);
      throw new RuntimeException("Failed to initialize Ranger Drill plugin — authorization disabled "+ serviceName + " with exception: " + e);
    }
  }

  /**
   * @return {@code true} if the Ranger plugin is initialized
   */
  public static boolean isEnabled() {
    return enabled;
  }

  /**
   * Resolves the OS-level groups for a given user via Hadoop UGI.
   *
   * @param user the username
   * @return a set of group names (never null, empty on failure)
   */
  // TODO: Currently group resolution relies on the OS-level mapping configured
  //   in Hadoop UserGroupInformation (e.g. /etc/group on Linux). This may
  //   diverge from the groups configured in Ranger Admin (e.g. a "meta" group
  //   that exists only in Ranger but not on the Drillbit host). Future work:
  //   - Sync Ranger user/group information into Drill so that policy group
  //     checks use Ranger as the source of truth instead of the local OS.
  //   - Cache Ranger group lookups with a configurable TTL to avoid hitting
  //     Ranger Admin on every access check.
  //   - Fall back to OS groups when Ranger Admin is unavailable, to keep
  //     authorization working during Ranger outages.
  public static Set<String> getUserGroups(String user) {
    if (user == null || user.trim().isEmpty()) {
      return Collections.emptySet();
    }
    try {
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
      String[] groups = ugi.getGroupNames();
      return groups == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(groups));
    } catch (Exception e) {
      LOG.warn("Failed to determine groups for user={}", user, e);
      return Collections.emptySet();
    }
  }

  /**
   * Checks table-level access. If Ranger is disabled, returns {@code true} (fail-open).
   * On Ranger evaluation error, returns {@code false} (fail-closed).
   *
   * @param user       the username requesting access
   * @param dataSource the Drill storage plugin name (e.g. "dfs", "hbase")
   * @param schema     the schema path (e.g. "dfs.tmp")
   * @param table      the table name
   * @param operator   the access type
   * @return {@code true} if access is allowed
   */
  public static boolean checkTableAccess(String user, String dataSource, String schema,
      String table, DrillAccessType operator) {
    if (!enabled || authorizer == null) {
      return true; // fail-open when disabled
    }
    // Bypass authorization for system schemas
    if (isSystemSchema(schema)) {
      return true;
    }
    try {
      DrillResource resource = new DrillResource();
      resource.setUser(user);
      resource.setGroups(getUserGroups(user));
      resource.setDataSource(dataSource != null ? dataSource : "drill");
      resource.setSchema(schema);
      resource.setTable(table);
      return authorizer.checkTableAccess(resource, operator);
    } catch (Exception e) {
      LOG.error("Checking table access for user={}, schema={}, table={}. with exception:{}", user, schema, table, e.toString());
      return false; // fail-closed on error
    }
  }

  /**
   * Convenience overload defaulting to {@link DrillAccessType#SELECT}.
   */
  public static boolean checkTableSelectAccess(String user, String dataSource, String schema, String table) {
    return checkTableAccess(user, dataSource, schema, table, DrillAccessType.SELECT);
  }

  /**
   * Checks whether a table should be visible to the user (for SHOW TABLES / metadata queries).
   * Returns {@code true} if the user has SELECT access OR if Ranger is disabled.
   *
   * @param user       the username
   * @param dataSource the Drill storage plugin name
   * @param schema     the schema path
   * @param table      the table name
   * @return {@code true} if the table is visible
   */
  public static boolean isTableVisible(String user, String dataSource, String schema, String table) {
    return checkTableAccess(user, dataSource, schema, table, DrillAccessType.SELECT);
  }

  // ========================================================================
  // DML/DDL operation-aware access checks
  //
  // IMPORTANT: INSERT and CTAS do NOT go through DrillTable.getGroupScan().
  // They have separate execution paths via AbstractSchema.createNewTable()
  // (CTAS) and AbstractSchema.modifyTable() (INSERT). These methods provide
  // the correct DrillAccessType for each DML/DDL operation, to be called at the
  // corresponding AbstractSchema hook points.
  // ========================================================================

  /**
   * Checks CREATE TABLE AS (CTAS) permission.
   * Hook point: {@code AbstractSchema.createNewTable()} — called by CreateTableHandler.
   *
   * @param user       the username
   * @param dataSource the Drill storage plugin name
   * @param schema     the schema path
   * @param table      the target table name
   * @return {@code true} if the user can create the table
   */
  public static boolean checkCreateTable(String user, String dataSource, String schema, String table) {
    return checkTableAccess(user, dataSource, schema, table, DrillAccessType.CREATE);
  }

  /**
   * Checks INSERT INTO permission.
   * Hook point: {@code AbstractSchema.modifyTable()} — called by TableModifyPrel.
   *
   * @param user       the username
   * @param dataSource the Drill storage plugin name
   * @param schema     the schema path
   * @param table      the target table name
   * @return {@code true} if the user can insert into the table
   */
  public static boolean checkInsert(String user, String dataSource, String schema, String table) {
    return checkTableAccess(user, dataSource, schema, table, DrillAccessType.INSERT);
  }

  /**
   * Checks DROP TABLE permission.
   * Hook point: {@code AbstractSchema.dropTable()} — called by DropTableHandler.
   *
   * @param user       the username
   * @param dataSource the Drill storage plugin name
   * @param schema     the schema path
   * @param table      the table name
   * @return {@code true} if the user can drop the table
   */
  public static boolean checkDropTable(String user, String dataSource, String schema, String table) {
    return checkTableAccess(user, dataSource, schema, table, DrillAccessType.DROP);
  }

  /**
   * Checks CREATE VIEW permission.
   * Hook point: {@code AbstractSchema.createView()} — called by ViewHandler.CreateView.
   *
   * @param user       the username
   * @param dataSource the Drill storage plugin name
   * @param schema     the schema path
   * @param viewName   the view name
   * @return {@code true} if the user can create the view
   */
  public static boolean checkCreateView(String user, String dataSource, String schema, String viewName) {
    return checkTableAccess(user, dataSource, schema, viewName, DrillAccessType.CREATE);
  }

  /**
   * Checks DROP VIEW permission.
   * Hook point: {@code AbstractSchema.dropView()} — called by ViewHandler.DropView.
   *
   * @param user       the username
   * @param dataSource the Drill storage plugin name
   * @param schema     the schema path
   * @param viewName   the view name
   * @return {@code true} if the user can drop the view
   */
  public static boolean checkDropView(String user, String dataSource, String schema, String viewName) {
    return checkTableAccess(user, dataSource, schema, viewName, DrillAccessType.DROP);
  }

  /**
   * Checks column-level access for a set of columns. Returns {@code true} only if the user
   * has access to ALL specified columns.
   *
   * @param user       the username
   * @param dataSource the Drill storage plugin name
   * @param schema     the schema path
   * @param table      the table name
   * @param columns    the set of column names to check
   * @param operator   the access type
   * @return {@code true} if access is allowed for every column
   */
  public static boolean checkColumnAccess(String user, String dataSource, String schema,
      String table, Set<String> columns, DrillAccessType operator) {
    if (!enabled || authorizer == null || isSystemSchema(schema)) {
      return true; // fail-open when disabled
    }

    try {
      DrillResource resource = new DrillResource();
      resource.setUser(user);
      resource.setGroups(getUserGroups(user));
      resource.setDataSource(dataSource != null ? dataSource : "drill");
      resource.setSchema(schema);
      resource.setTable(table);
      resource.setColumns(columns);
      return authorizer.checkColumnAccess(resource, operator);
    } catch (Exception e) {
      LOG.error("Error checking column access for user={}, schema={}, table={}", user, schema, table, e);
      return false; // fail-closed on error
    }
  }

  /**
   * Returns whether the given schema is a system schema that should bypass authorization.
   *
   * <p>Comparison is case-insensitive so that SQL like
   * {@code SELECT * FROM information_schema.tables} (lowercase) or
   * {@code SELECT * FROM SYS.DRILLBITS} (uppercase) both bypass authorization,
   * matching Drill's own case-insensitive schema resolution.
   *
   * <p>For compound schema paths like {@code dfs.tmp}, only the top-level segment
   * (the storage plugin name) is checked — that is intentional, because system
   * schemas ({@code INFORMATION_SCHEMA}, {@code sys}) are always top-level.
   */
  private static boolean isSystemSchema(String schema) {
    if (schema == null || schema.trim().isEmpty()) {
      return true;
    }
    // Use only the top-level segment of a compound schema path
    // (e.g. "dfs.tmp" -> "dfs", "INFORMATION_SCHEMA" -> "INFORMATION_SCHEMA")
    String topLevel = schema;
    int dot = schema.indexOf('.');
    if (dot > 0) {
      topLevel = schema.substring(0, dot);
    }
    return SYSTEM_SCHEMAS.contains(topLevel.toUpperCase());
  }
}
