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

/**
 * Access type string constants used as the {@code accessType} argument to
 * {@link AccessAuthorizer#checkTableAccess} and
 * {@link AccessAuthorizer#checkColumnAccess}.
 *
 * <p>These constants mirror the values of {@code DrillAccessType} enum in the
 * {@code drill-ranger-plugin} module. The main classpath cannot see that enum
 * (it lives behind the {@code RangerPluginClassLoader} isolation boundary),
 * so these string constants are the only way for Drill core to reference an
 * access type.</p>
 *
 * <p>At runtime, {@code DrillAccessControl.checkTableAccess(..., String)}
 * converts the string to {@code DrillAccessType} via
 * {@code DrillAccessType.valueOf(operator.toUpperCase())}. If a constant here
 * does not match an enum value (e.g. typo or drift after a Ranger upgrade),
 * the conversion fails and access is denied (fail-closed). This means the
 * string constants do not need to be kept in perfect lock-step with the enum
 * — a mismatch is caught at the first call rather than silently allowing
 * unauthorized access.</p>
 *
 * <p>Using a constant class (rather than a dedicated semantic method per
 * access type like {@code checkTableSelectAccess},
 * {@code checkTableInsertAccess}, ...) keeps the {@link AccessAuthorizer}
 * interface compact as new access types are added. A new operation only
 * requires adding a constant here; callers use the generic
 * {@code checkTableAccess(..., AccessTypes.INSERT)} form.</p>
 */
public final class AccessTypes {

  private AccessTypes() {
  }

  public static final String SELECT  = "SELECT";
  public static final String CREATE  = "CREATE";
  public static final String INSERT  = "INSERT";
  public static final String DROP    = "DROP";
  public static final String USE     = "USE";
  public static final String DELETE  = "DELETE";
  public static final String SHOW    = "SHOW";
}
