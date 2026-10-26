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

/**
 * Access type for {@link AccessAuthorizer#checkTableAccess} and
 * {@link AccessAuthorizer#checkColumnAccess} — mirrors Presto's
 * {@code io.prestosql.spi.security.Privilege} enum.
 *
 * <p>The enum keeps the set of access types a closed, compile-time-checked
 * vocabulary: a typo like {@code "SELEC"} is a compile error instead of a
 * run-time denial. Adding a constant is binary-compatible (implementations
 * keep working; unknown values are denied fail-closed by implementations
 * such as the Ranger plugin, which maps this enum to its own
 * {@code DrillAccessType} by name).</p>
 *
 * <p>The Drill engine issues {@link #SELECT} for table- and column-level
 * checks during SQL validation, and {@link #CREATE} / {@link #DROP} for DDL
 * authorization (CREATE TABLE / CTAS, CREATE VIEW from the DDL handlers;
 * DROP TABLE / DROP VIEW from their handlers). Temporary tables are
 * session-scoped and bypass these checks.</p>
 */
public enum AccessType {
  SELECT,
  CREATE,
  DROP
}
