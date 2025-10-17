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
 * Compatibility shim for Calcite 1.37+ migration.
 *
 * In Calcite 1.36, RAND was implemented as a dedicated SqlRandFunction class extending SqlFunction.
 * In Calcite 1.37, RAND became a SqlBasicFunction in SqlStdOperatorTable.
 *
 * This class provides backward compatibility for deserializing view definitions
 * that were created with Calcite 1.36 and contain serialized SqlRandFunction references.
 *
 * When Java deserializes an old view definition and encounters
 * "org.apache.calcite.sql.fun.SqlRandFunction", it will load this shim class instead.
 * The readResolve() method ensures that the deserialized object is replaced with
 * the current Calcite 1.37 RAND implementation from SqlStdOperatorTable.
 */
public class SqlRandFunction extends SqlFunction {

  /**
   * Constructor matching the original SqlRandFunction signature from Calcite 1.36.
   * This is needed for deserialization to work properly.
   */
  public SqlRandFunction() {
    super("RAND",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE,
        null,
        OperandTypes.or(OperandTypes.NILADIC, OperandTypes.NUMERIC),
        SqlFunctionCategory.NUMERIC);
  }

  /**
   * Matches the original Calcite 1.36 behavior where RAND was marked as non-deterministic.
   */
  @Override
  public boolean isDynamicFunction() {
    return true;
  }

  /**
   * Serialization replacement method.
   * When this object is deserialized, Java will call readResolve() and replace
   * this shim instance with the actual Calcite 1.37 RAND function.
   *
   * @return The current RAND implementation from SqlStdOperatorTable
   */
  private Object readResolve() {
    return SqlStdOperatorTable.RAND;
  }
}
