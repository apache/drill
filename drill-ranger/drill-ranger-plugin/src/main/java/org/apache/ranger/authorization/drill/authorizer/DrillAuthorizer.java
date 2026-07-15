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

import org.apache.ranger.authorization.drill.resource.DrillAccessResource;
import org.apache.ranger.authorization.drill.resource.DrillAccessType;
import org.apache.ranger.authorization.drill.resource.DrillRangerAccessRequest;
import org.apache.ranger.authorization.drill.resource.DrillResource;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class DrillAuthorizer {
  private static final Logger LOG = LoggerFactory.getLogger(DrillAuthorizer.class);
  private RangerBaseAuthorizer authorizer;

  public DrillAuthorizer(String serviceName) {
    authorizer = RangerBaseAuthorizer.getInstance();
    authorizer.init(serviceName);
  }

  private boolean checkPermission(DrillRangerAccessRequest request) {
    return authorizer.isAccessAllowed(request.toRangerRequest());
  }

  /**
   * Build a DrillRangerAccessRequest from the given resource and access type, then check
   * permission. This abstracts the common logic shared by table-level and column-level
   * access checks.
   *
   * @param resource           the DrillResource providing user, groups, etc.
   * @param drillAccessResource the DrillAccessResource describing the accessed entity
   * @param operator           the access type to check
   * @return the permission check result
   */
  private boolean checkAccess(DrillResource resource, DrillAccessResource drillAccessResource,
      DrillAccessType operator, RangerAccessRequest.ResourceMatchingScope scope) {
    Set<String> groups = new HashSet<>();
    if (resource.getGroups() != null) {
      groups.addAll(resource.getGroups());
    }

    DrillRangerAccessRequest request = DrillRangerAccessRequest.builder()
        .user(resource.getUser())
        .groups(groups)
        .resource(drillAccessResource)
        .accessType(operator)
        .resourceMatchingScope(scope)
        .build();

    return checkPermission(request);
  }

  public boolean checkTableAccess(DrillResource resource, DrillAccessType operator) {
    if (!validateResource(resource, ValidationLevel.TABLE)) {
      LOG.warn("MetaStoreResource validation failed for table access check");
      return false;
    }
    Optional<String> schema = Optional.ofNullable(resource.getSchema());
    Optional<String> table = Optional.ofNullable(resource.getTable());
    DrillAccessResource drillAccessResource = new DrillAccessResource(resource.getDataSource(),
        schema, table);

    // Table-level check uses SELF_OR_DESCENDANTS so a request without a column
    // can still match column-level policies (column is a descendant of table).
    // This allows a single policy with column=amount to authorize the table-level
    // SELECT check that happens during SQL parsing (before columns are resolved).
    boolean result = checkAccess(resource, drillAccessResource, operator,
        RangerAccessRequest.ResourceMatchingScope.SELF_OR_DESCENDANTS);

    LOG.info("checkTableAccess result for user={}, datasource={}, schema={}, table={}, " +
            "operator={}: result={}",
        resource.getUser(), resource.getDataSource(), resource.getSchema(),
        resource.getTable(), operator.name(), result);

    return result;
  }

  /**
   * Validate that required fields of MetaStoreResource are non-empty (full validation, defaults
   * to column level)
   *
   * @param resource the resource object to validate
   * @return true if validation passes, false otherwise
   */
  public boolean validateResource(DrillResource resource) {
    return validateResource(resource, ValidationLevel.COLUMN);
  }

  public boolean checkColumnAccess(DrillResource resource, DrillAccessType operator) {
    if (!validateResource(resource, ValidationLevel.COLUMN)) {
      LOG.warn("MetaStoreResource validation failed for table access check");
      return false;
    }
    Optional<String> schema = Optional.ofNullable(resource.getSchema());
    Optional<String> table = Optional.ofNullable(resource.getTable());

    for (String column : resource.getColumns()) {
      Optional<String> columnOpt = Optional.of(column);
      DrillAccessResource drillAccessResource = new DrillAccessResource(resource.getDataSource(), schema, table, columnOpt);

      // Column-level check uses SELF for exact column matching: only policies
      // whose column resource matches the requested column will be applied.
      boolean allowed = checkAccess(resource, drillAccessResource, operator,
          RangerAccessRequest.ResourceMatchingScope.SELF);

      LOG.info("checkColumnAccess result for user={}, datasource={}, schema={}, table={}, " +
              "column={}, operator={}: result={}",
          resource.getUser(), resource.getDataSource(), resource.getSchema(),
          resource.getTable(), column, operator.name(), allowed);

      if (!allowed) {
        // Fail fast on first denied column — no need to check the rest.
        LOG.warn("Column access denied for user={}, column={}.{}.{}",
            resource.getUser(), resource.getDataSource(), resource.getSchema(),
            resource.getTable());
        return false;
      }
    }
    return true;
  }

  /**
   * Validate that required fields of MetaStoreResource are non-empty (with specified validation
   * level)
   *
   * @param resource        the resource object to validate
   * @param validationLevel the validation level
   * @return true if validation passes, false otherwise
   */
  private boolean validateResource(DrillResource resource, ValidationLevel validationLevel) {
    boolean allValid = true;
    if (resource == null) {
      LOG.error("MetaStoreResource is null");
      return false;
    }
    if (resource.getUser() == null || resource.getUser().trim().isEmpty()) {
      LOG.error("MetaStoreResource user is null or empty");
      return false;
    }
    if (resource.getDataSource() == null || resource.getDataSource().trim().isEmpty()) {
      LOG.error("MetaStoreResource dataSource is null or empty");
      return false;
    }
    if (validationLevel.ordinal() >= ValidationLevel.SCHEMA.ordinal()) {
      if (resource.getSchema() == null || resource.getSchema().trim().isEmpty()) {
        LOG.error("MetaStoreResource schema is null or empty");
        return false;
      }
    }
    if (validationLevel.ordinal() >= ValidationLevel.TABLE.ordinal()) {
      if (resource.getTable() == null || resource.getTable().trim().isEmpty()) {
        LOG.error("MetaStoreResource table is null or empty");
        return false;
      }
    }
    if (validationLevel.ordinal() >= ValidationLevel.COLUMN.ordinal()) {
      if (resource.getColumns() == null || resource.getColumns().isEmpty()) {
        LOG.error("MetaStoreResource columns is null or empty");
        return false;
      }
      allValid = resource.getColumns().stream()
          .allMatch(column -> column != null && !column.trim().isEmpty());
    }
    return allValid;
  }
}
