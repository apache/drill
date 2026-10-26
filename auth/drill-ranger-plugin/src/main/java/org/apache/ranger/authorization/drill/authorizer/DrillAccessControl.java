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
package org.apache.ranger.authorization.drill.authorizer;


import org.apache.drill.exec.security.spi.AccessAuthorizer;
import org.apache.drill.exec.security.spi.AccessType;
import org.apache.drill.exec.security.spi.UserIdentity;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.drill.resource.DrillAccessType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Drill-facing authorization facade: implementation of the Drill
 * {@link AccessAuthorizer} SPI backed by Ranger.
 *
 * <p>The class is instantiated once by the shim (drill-ranger-plugin-shim)
 * via {@code DrillAccessControl(String serviceName)} — the constructor
 * completes all initialization (or throws, fail-closed), mirroring Presto's
 * {@code RangerPrestoAccessControl}. All access checks are instance methods
 * invoked directly through the SPI interface.</p>
 *
 * <p>Group resolution: if the engine-supplied {@link UserIdentity} carries
 * groups (resolved at authentication time), they are used as-is; otherwise
 * the groups are resolved via Hadoop UGI ({@link #getUserGroups}).</p>
 *
 * <p>On Ranger evaluation error, checks return {@code false} (fail-closed).
 * The service name identifies the Ranger service instance whose policies
 * are evaluated.</p>
 */
public class DrillAccessControl implements AccessAuthorizer {

  private static final Logger logger = LoggerFactory.getLogger(DrillAccessControl.class);

  private final DrillAuthorizer authorizer;

  // Set of system schemas that bypass authorization (information_schema, sys, etc.)
  // Stored in uppercase; isSystemSchema() uppercases input before lookup so the
  // bypass is case-insensitive (e.g. "information_schema", "INFORMATION_SCHEMA",
  // "Sys", "SYS" all match).
  private static final Set<String> SYSTEM_SCHEMAS = new HashSet<>(Arrays.asList(
      "INFORMATION_SCHEMA", "SYS"
  ));

  /**
   * Creates and initializes the Ranger Drill plugin. Completes all
   * initialization or throws (fail-closed) — the caller (the shim) must not
   * receive a half-initialized authorizer.
   *
   * @param serviceName the Ranger service instance name (must match a service created in Ranger Admin)
   */
  public DrillAccessControl(String serviceName) {
    logger.info("Initializing Ranger Drill authorization plugin for service: {}", serviceName);
    try {
      this.authorizer = new DrillAuthorizer(serviceName);
      logger.info("Ranger Drill authorization plugin initialized successfully");
    } catch (Exception e) {
      logger.error("Failed to initialize Ranger Drill plugin for service {}", serviceName, e);
      throw new RuntimeException(
          "Failed to initialize Ranger Drill plugin — authorization disabled " + serviceName
              + " with exception: " + e);
    }
  }

  /**
   * Package-private constructor for unit tests: injects a (mock) authorizer
   * directly, bypassing Ranger Admin connectivity.
   */
  DrillAccessControl(DrillAuthorizer authorizer) {
    this.authorizer = authorizer;
  }

  /**
   * Resolves the OS-level groups for a given user via Hadoop UGI. Used only
   * as a fallback when the engine-supplied {@link UserIdentity} carries no
   * groups.
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
   * Checks table-level access. The SPI {@link AccessType} is mapped to
   * {@link DrillAccessType} by name; if the mapping fails (e.g. the SPI
   * added a type this plugin does not know yet) access is denied
   * (fail-closed).
   *
   * <p>System schemas ({@code INFORMATION_SCHEMA}, {@code sys}) bypass
   * authorization. A null/empty schema is invalid input and fails closed
   * rather than silently bypassing Ranger.</p>
   *
   * @param user       the querying user identity
   * @param dataSource the Drill storage plugin name (e.g. "dfs", "hbase")
   * @param schema     the schema path (e.g. "dfs.tmp")
   * @param table      the table name
   * @param accessType the access type (e.g. SELECT, CREATE)
   * @return {@code true} if access is allowed
   */
  @Override
  public boolean checkTableAccess(UserIdentity user, String dataSource, String schema,
      String table, AccessType accessType) {
    DrillAccessType operator = parseAccessType(accessType, user, schema, table);
    if (operator == null) {
      return false;
    }
    Boolean bypass = systemSchemaBypass(user, schema, table);
    if (bypass != null) {
      return bypass;
    }
    try {
      return authorizer.checkTableAccess(resolveIdentity(user), dataSource, schema, table, operator);
    } catch (Exception e) {
      logger.error("Checking table access for user={}, schema={}, table={}. with exception:{}",
          user.getUser(), schema, table, e.toString());
      return false; // fail-closed on error
    }
  }

  /**
   * Checks column-level access for a set of columns. Returns {@code true}
   * only if the user has the specified access type on ALL given columns.
   *
   * <p>System schemas ({@code INFORMATION_SCHEMA}, {@code sys}) bypass
   * authorization. A null/empty schema is invalid input and fails closed
   * rather than silently bypassing Ranger.</p>
   *
   * @param user       the querying user identity
   * @param dataSource the Drill storage plugin name
   * @param schema     the schema path
   * @param table      the table name
   * @param columns    the set of column names to check
   * @param accessType the access type (e.g. SELECT)
   * @return {@code true} if access is allowed for every column
   */
  @Override
  public boolean checkColumnAccess(UserIdentity user, String dataSource, String schema,
      String table, Set<String> columns, AccessType accessType) {
    DrillAccessType operator = parseAccessType(accessType, user, schema, table);
    if (operator == null) {
      return false;
    }
    Boolean bypass = systemSchemaBypass(user, schema, table);
    if (bypass != null) {
      return bypass;
    }
    try {
      return authorizer.checkColumnAccess(resolveIdentity(user), dataSource, schema, table,
          columns, operator);
    } catch (Exception e) {
      logger.error("Error checking column access for user={}, schema={}, table={}",
          user.getUser(), schema, table, e);
      return false; // fail-closed on error
    }
  }

  /**
   * Maps the SPI {@link AccessType} to a {@link DrillAccessType} by name.
   * Returns {@code null} (and logs) when the SPI enum carries a type this
   * plugin does not know yet — callers deny access in that case
   * (fail-closed). This guards against drift when a newer SPI adds access
   * types before the Ranger service-def does.
   */
  private DrillAccessType parseAccessType(AccessType accessType, UserIdentity user,
      String schema, String table) {
    try {
      return DrillAccessType.valueOf(accessType.name());
    } catch (Exception e) {
      logger.error("Unsupported access type '{}', denied access for user={}, schema={}, table={}",
          accessType, user.getUser(), schema, table);
      return null;
    }
  }

  /**
   * Resolves the effective identity for an access check: engine-supplied
   * groups are used as-is; when the identity carries none (e.g. mount points
   * that only have the authenticated user name), the groups are resolved via
   * Hadoop UGI ({@link #getUserGroups}).
   */
  private UserIdentity resolveIdentity(UserIdentity user) {
    Set<String> groups = user.getGroups();
    if (groups != null && !groups.isEmpty()) {
      return user;
    }
    return UserIdentity.builder()
        .setUser(user.getUser())
        .setGroups(getUserGroups(user.getUser()))
        .build();
  }

  /**
   * Returns whether the given schema is a system schema that should bypass
   * authorization.
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

  /**
   * Shared guard for the check methods: returns {@link Boolean#TRUE} for a
   * system schema (bypass authorization), {@link Boolean#FALSE} for a
   * malformed (null/empty) schema (fail closed — never silently bypass
   * Ranger), or {@code null} when the check should proceed to the authorizer.
   */
  private Boolean systemSchemaBypass(UserIdentity user, String schema, String table) {
    try {
      return isSystemSchema(schema) ? Boolean.TRUE : null;
    } catch (IllegalArgumentException e) {
      logger.error("Malformed (null/empty) schema in access check for user={}, table={}: {}",
          user.getUser(), table, e.getMessage());
      return Boolean.FALSE; // malformed schema: fail closed
    }
  }

  /**
   * Releases the Ranger plugin resources held by the underlying authorizer
   * (policy-refresh threads, policy-engine caches). Forwarded from the shim
   * ({@code RangerAccessAuthorizer}) when the Drillbit shuts down. Idempotent
   * and safe if the plugin was never initialized.
   */
  @Override
  public void close() {
    logger.info("Closing Ranger Drill authorization plugin");
    authorizer.close();
  }
}
