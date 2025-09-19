/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Compatibility class for Apache Drill's integration with Calcite 1.40.
 * This class provides the SqlRandFunction that was removed from Calcite 1.40
 * but is still referenced by Drill's codebase during runtime.
 *
 * In Calcite 1.40, the RAND function implementation was moved to
 * org.apache.calcite.runtime.RandomFunction, but some internal
 * mechanisms still expect this SQL function class to exist.
 */
public class SqlRandFunction extends SqlFunction {

  public SqlRandFunction() {
    super("RAND",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE,
        null,
        OperandTypes.or(OperandTypes.NILADIC, OperandTypes.NUMERIC),
        SqlFunctionCategory.NUMERIC);
  }
}