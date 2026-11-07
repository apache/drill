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

import org.apache.drill.exec.security.spi.AccessAuthorizer;
import org.apache.drill.exec.security.spi.AccessType;
import org.apache.drill.exec.security.spi.UserIdentity;

import java.util.Set;

/**
 * Engine-default implementation that allows all access (mirrors Presto's
 * {@code AllowAllAccessControl}). Used when the access authorizer is
 * disabled: the engine calls it through the same {@link AccessAuthorizer}
 * interface, so mount points need no special-casing for the disabled state.
 */
public class AllowAllAccessAuthorizer implements AccessAuthorizer {

  @Override
  public boolean checkTableAccess(UserIdentity user, String dataSource, String schema,
                                  String table, AccessType accessType) {
    return true; // fail-open
  }

  @Override
  public boolean checkColumnAccess(UserIdentity user, String dataSource, String schema,
                                   String table, Set<String> columns, AccessType accessType) {
    return true; // fail-open
  }
}
