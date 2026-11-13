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
package org.apache.drill.exec.security;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.security.spi.AccessAuthorizer;
import org.apache.drill.exec.security.spi.AccessType;
import org.apache.drill.exec.security.spi.UserIdentity;
import org.apache.drill.exec.store.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DDL authorization checks for the handlers that create or drop tables and
 * views ({@code CREATE TABLE} / CTAS, {@code DROP TABLE}, {@code CREATE VIEW},
 * {@code DROP VIEW}).
 *
 * <p>Statements that go through the Calcite catalog reader (queries, and the
 * query part of CTAS / CREATE VIEW) are checked in
 * {@code DrillCalciteCatalogReader}. DDL statements address their target
 * object directly through the schema, so the check is issued here at the
 * handler layer.</p>
 *
 * <p>Resource mapping reuses {@link TableAccessResource#resolve(List)} so
 * table-level, column-level and DDL checks address exactly the same resource
 * for the same table. No-op when authorization is disabled (the manager
 * returns the allow-all authorizer).</p>
 */
public final class DdlAccessChecker {

  private static final Logger logger = LoggerFactory.getLogger(DdlAccessChecker.class);

  private DdlAccessChecker() {
  }

  /**
   * Checks CREATE/DROP permission on the DDL target object (table or view)
   * via the configured {@link AccessAuthorizer} (Ranger by default). Throws
   * {@link UserException} permissionError when access is denied.
   *
   * <p>Authorization happens before the existence check ("table not found")
   * so that an unauthorized user cannot probe object existence through
   * differing error messages.</p>
   *
   * @param context    query context (session + config)
   * @param schema     the resolved schema holding the target object
   * @param objectName the target table/view name as written by the user
   * @param accessType the access type ({@link AccessType#CREATE} or
   *                   {@link AccessType#DROP})
   */
  public static void checkDdlAccess(QueryContext context, AbstractSchema schema,
                                    String objectName, AccessType accessType) {
    List<String> qualifiedName = new ArrayList<>(schema.getSchemaPath());
    qualifiedName.add(objectName);
    TableAccessResource resource = TableAccessResource.resolve(qualifiedName);
    String userName = context.getSession().getCredentials().getUserName();
    DrillConfig drillConfig = context.getConfig();
    AccessAuthorizer authorizer = AccessAuthorizerManager.getAuthorizer(drillConfig);
    if (!authorizer.checkTableAccess(UserIdentity.of(userName), resource.getDataSource(),
        resource.getSchemaPath(), resource.getTable(), accessType)) {
      throw UserException.permissionError()
          .message("Access denied: user '%s' lacks %s privilege on %s",
              userName, accessType, resource)
          .build(logger);
    }
  }
}
