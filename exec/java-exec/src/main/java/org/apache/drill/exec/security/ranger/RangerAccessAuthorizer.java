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

import org.apache.ranger.authorization.drill.authorizer.DrillAccessControl;
import org.apache.ranger.authorization.drill.resource.DrillAccessType;

import java.util.Set;

/**
 * {@link AccessAuthorizer} SPI implementation backed by Ranger.
 *
 * <p>This class is the bridge between Drill core (which only depends on the
 * {@code AccessAuthorizer} interface in {@code drill-java-exec}) and the Ranger
 * plugin (this module). All calls are delegated to {@link DrillAccessControl},
 * the static facade that owns the fail-open/fail-closed policy and system-schema
 * bypass logic.</p>
 */
public class RangerAccessAuthorizer implements AccessAuthorizer {

  @Override
  public void init(String serviceName) {
    DrillAccessControl.init(serviceName);
  }

  @Override
  public boolean isEnabled() {
    return DrillAccessControl.isEnabled();
  }

  @Override
  public boolean checkTableAccess(String user, String dataSource, String schema,
                                  String table, String accessType) {
    DrillAccessType operator = toAccessType(accessType);
    return DrillAccessControl.checkTableAccess(user, dataSource, schema, table, operator);
  }

  @Override
  public boolean checkColumnAccess(String user, String dataSource, String schema,
                                   String table, Set<String> columns, String accessType) {
    DrillAccessType operator = toAccessType(accessType);
    return DrillAccessControl.checkColumnAccess(user, dataSource, schema, table, columns, operator);
  }

  private static DrillAccessType toAccessType(String accessType) {
    if (accessType == null) {
      return DrillAccessType.SELECT;
    }
    try {
      return DrillAccessType.valueOf(accessType.toUpperCase());
    } catch (IllegalArgumentException e) {
      return DrillAccessType.SELECT; // unknown type defaults to SELECT
    }
  }
}
