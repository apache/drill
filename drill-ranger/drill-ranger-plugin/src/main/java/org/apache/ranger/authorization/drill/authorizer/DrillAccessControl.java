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

  private static final Logger logger = LoggerFactory.getLogger(DrillAccessControl.class);

  private static volatile boolean enabled = false;
  private static volatile DrillAuthorizer authorizer;

  // Set of system schemas that bypass authorization (information_schema, sys, etc.)
  // Stored in uppercase; isSystemSchema() uppercases input before lookup so the
  // bypass is case-insensitive (e.g. "information_schema", "INFORMATION_SCHEMA",
  // "Sys", "SYS" all match).
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
      logger.info("Initializing Ranger Drill authorization plugin for service: {}", serviceName);
      authorizer = new DrillAuthorizer(serviceName);
      enabled = true;
      logger.info("Ranger Drill authorization plugin initialized successfully");
    } catch (Exception e) {
      logger.error("Failed to initialize Ranger Drill plugin — authorization DISABLED", e);
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
  public static Set<String> getUserGroups(String user) {
    if (user == null || user.trim().isEmpty()) {
      return Collections.emptySet();
    }
    try {
      UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
      String[] groups = ugi.getGroupNames();
      return groups == null ? Collections.emptySet() : new HashSet<>(Arrays.asList(groups));
    } catch (Exception e) {
      logger.warn("Failed to determine groups for user={}", user, e);
      return Collections.emptySet();
    }
  }

  /**
   * Checks table-level access. This is the external entry point that accepts
   * the operator as a string (e.g. "SELECT", "CREATE"). The string is
   * converted to {@link DrillAccessType}; if the conversion fails the
   * operator is not a supported access type and access is denied.
   *
   * @param user       the username requesting access
   * @param dataSource the Drill storage plugin name (e.g. "dfs", "hbase")
   * @param schema     the schema path (e.g. "dfs.tmp")
   * @param table      the table name
   * @param operator   the access type string (e.g. "SELECT", "CREATE")
   * @return {@code true} if access is allowed
   */
  public static boolean checkTableAccess(String user, String dataSource, String schema,
      String table, String operator) {
    DrillAccessType accessType;
    try {
      accessType = DrillAccessType.valueOf(operator.toUpperCase());
    } catch (Exception e) {
      logger.error("Unsupported access type '{}', denied table access for user={}, schema={}, table={}",
          operator, user, schema, table);
      return false;
    }
    return checkTableAccess(user, dataSource, schema, table, accessType);
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
    // Bypass authorization for system schemas; a null/empty schema is invalid input
    // and fails closed rather than silently bypassing Ranger.
    boolean systemSchema;
    try {
      systemSchema = isSystemSchema(schema);
    } catch (IllegalArgumentException e) {
      logger.error("Malformed (null/empty) schema in table access check for user={}, table={}: {}",
          user, table, e.getMessage());
      return false;
    }
    if (systemSchema) {
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
      logger.error("Checking table access for user={}, schema={}, table={}. with exception:{}", user, schema, table, e.toString());
      return false; // fail-closed on error
    }
  }

  /**
   * Checks column-level access for a set of columns. This is the external entry
   * point that accepts the operator as a string. The string is converted to
   * {@link DrillAccessType}; if the conversion fails the operator is not a
   * supported access type and access is denied.
   *
   * @param user       the username
   * @param dataSource the Drill storage plugin name
   * @param schema     the schema path
   * @param table      the table name
   * @param columns    the set of column names to check
   * @param operator   the access type string (e.g. "SELECT")
   * @return {@code true} if access is allowed for every column
   */
  public static boolean checkColumnAccess(String user, String dataSource, String schema,
      String table, Set<String> columns, String operator) {
    DrillAccessType accessType;
    try {
      accessType = DrillAccessType.valueOf(operator.toUpperCase());
    } catch (Exception e) {
      logger.error("Unsupported access type '{}', denied column access for user={}, schema={}, table={}",
          operator, user, schema, table);
      return false;
    }
    return checkColumnAccess(user, dataSource, schema, table, columns, accessType);
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
    if (!enabled || authorizer == null) {
      return true; // fail-open when disabled
    }
    // Bypass authorization for system schemas; a null/empty schema is invalid input
    // and fails closed rather than silently bypassing Ranger.
    boolean systemSchema;
    try {
      systemSchema = isSystemSchema(schema);
    } catch (IllegalArgumentException e) {
      logger.error("Malformed (null/empty) schema in column access check for user={}, table={}: {}",
          user, table, e.getMessage());
      return false;
    }
    if (systemSchema) {
      return true;
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
      logger.error("Error checking column access for user={}, schema={}, table={}", user, schema, table, e);
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
      throw new IllegalArgumentException(
          "Schema must not be null or empty for authorization check; refusing to treat as system schema");
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
