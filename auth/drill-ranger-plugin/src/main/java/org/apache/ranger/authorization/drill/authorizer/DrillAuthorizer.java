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

import org.apache.drill.exec.security.spi.UserIdentity;
import org.apache.ranger.authorization.drill.resource.DrillAccessResource;
import org.apache.ranger.authorization.drill.resource.DrillAccessType;
import org.apache.ranger.authorization.drill.resource.DrillRangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Set;

/**
 * Ranger-side authorization adapter: builds {@link RangerAccessRequest}s from
 * plain check parameters and evaluates them against the Ranger policy engine.
 *
 * <p>Responsibilities: input validation (fail-closed on malformed arguments),
 * per-column request iteration, and resource-matching-scope semantics —
 * table-level checks use {@code SELF_OR_DESCENDANTS} so a table request can
 * match column-level policies, column-level checks use {@code SELF} for exact
 * column matching.</p>
 */
public class DrillAuthorizer {
  private static final Logger logger = LoggerFactory.getLogger(DrillAuthorizer.class);
  private RangerBaseAuthorizer authorizer;

  public DrillAuthorizer(String serviceName) {
    authorizer = RangerBaseAuthorizer.getInstance();
    authorizer.init(serviceName);
  }

  /**
   * Checks table-level access.
   *
   * @param user       the querying user identity (with groups already resolved)
   * @param dataSource the Drill storage plugin name (e.g. "dfs")
   * @param schema     the schema path (e.g. "dfs.tmp")
   * @param table      the table name
   * @param operator   the access type to check
   * @return {@code true} if access is allowed; {@code false} on denial or
   *         malformed input (fail-closed)
   */
  public boolean checkTableAccess(UserIdentity user, String dataSource, String schema,
      String table, DrillAccessType operator) {
    if (!validate(user, dataSource, schema, table)) {
      logger.warn("Table access check denied: invalid arguments for user={}, datasource={}, schema={}, table={}",
          user == null ? null : user.getUser(), dataSource, schema, table);
      return false;
    }
    DrillAccessResource resource = new DrillAccessResource(dataSource,
        Optional.ofNullable(schema), Optional.ofNullable(table));

    // Table-level check uses SELF_OR_DESCENDANTS so a request without a column
    // can still match column-level policies (column is a descendant of table).
    // This allows a single policy with column=amount to authorize the table-level
    // SELECT check that happens during SQL parsing (before columns are resolved).
    boolean result = checkAccess(user, resource, operator,
        RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);
    if (logger.isDebugEnabled()) {
      logger.debug("checkTableAccess result for user={}, datasource={}, schema={}, table={}, " +
              "operator={}: result={}",
          user.getUser(), dataSource, schema, table, operator.name(), result);
    }
    return result;
  }

  /**
   * Checks column-level access for a set of columns. Each column is checked
   * individually (fail-fast on the first denial).
   *
   * @param user       the querying user identity (with groups already resolved)
   * @param dataSource the Drill storage plugin name (e.g. "dfs")
   * @param schema     the schema path (e.g. "dfs.tmp")
   * @param table      the table name
   * @param columns    the column names to check
   * @param operator   the access type to check
   * @return {@code true} if access is allowed for every column; {@code false}
   *         on denial or malformed input (fail-closed)
   */
  public boolean checkColumnAccess(UserIdentity user, String dataSource, String schema,
      String table, Set<String> columns, DrillAccessType operator) {
    if (!validate(user, dataSource, schema, table) || !validColumns(columns)) {
      logger.warn("Column access check denied: invalid arguments for user={}, datasource={}, schema={}, table={}",
          user == null ? null : user.getUser(), dataSource, schema, table);
      return false;
    }
    Optional<String> schemaOpt = Optional.ofNullable(schema);
    Optional<String> tableOpt = Optional.ofNullable(table);

    for (String column : columns) {
      DrillAccessResource resource = new DrillAccessResource(dataSource,
          schemaOpt, tableOpt, Optional.of(column));

      // Column-level check uses SELF for exact column matching: only policies
      // whose column resource matches the requested column will be applied.
      boolean allowed = checkAccess(user, resource, operator,
          RangerAccessRequest.ResourceMatchingScope.SELF);
      if (logger.isDebugEnabled()) {
        logger.debug("checkColumnAccess result for user={}, datasource={}, schema={}, table={}, " +
                "column={}, operator={}: result={}",
            user.getUser(), dataSource, schema, table, column, operator.name(), allowed);
      }
      if (!allowed) {
        // Fail fast on first denied column — no need to check the rest.
        logger.warn("Column access denied for user={}, column={}.{}.{}",
            user.getUser(), dataSource, schema, table, column);
        return false;
      }
    }
    return true;
  }

  /**
   * Releases the Ranger plugin resources owned by the singleton
   * {@link RangerBaseAuthorizer} (policy-refresh threads, policy-engine
   * caches). Idempotent; safe when the plugin was never initialized. Called
   * through {@link DrillAccessControl#close()} when the Drillbit shuts down.
   */
  public void close() {
    authorizer.cleanUp();
  }

  /**
   * Builds a {@link DrillRangerAccessRequest} from the identity, resource and
   * access type, then evaluates it against the Ranger policy engine.
   */
  private boolean checkAccess(UserIdentity user, DrillAccessResource drillAccessResource,
      DrillAccessType operator, RangerAccessRequest.ResourceMatchingScope scope) {
    DrillRangerAccessRequest request = DrillRangerAccessRequest.builder()
        .user(user.getUser())
        .groups(user.getGroups())
        .resource(drillAccessResource)
        .accessType(operator)
        .resourceMatchingScope(scope)
        .build();

    return authorizer.isAccessAllowed(request.toRangerRequest());
  }

  /**
   * Validates the arguments shared by table- and column-level checks: the
   * identity, its user name, dataSource, schema and table must all be
   * non-null and non-empty (fail-closed on malformed input).
   */
  private boolean validate(UserIdentity user, String dataSource, String schema, String table) {
    return user != null
        && user.getUser() != null && !user.getUser().trim().isEmpty()
        && dataSource != null && !dataSource.trim().isEmpty()
        && schema != null && !schema.trim().isEmpty()
        && table != null && !table.trim().isEmpty();
  }

  /**
   * Validates the column set: non-null, non-empty, and every column name
   * non-null and non-empty.
   */
  private boolean validColumns(Set<String> columns) {
    return columns != null && !columns.isEmpty()
        && columns.stream().allMatch(c -> c != null && !c.trim().isEmpty());
  }
}
