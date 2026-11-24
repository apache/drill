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
package org.apache.drill.exec.planner.sql.conversion;

import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

/** SQL parameter that retains its value for the first physical plan build. */
public final class SqlBoundDynamicParam extends SqlDynamicParam {
  private final SqlLiteral literal;

  public SqlBoundDynamicParam(int index, SqlParserPos pos, SqlLiteral literal) {
    super(index, pos);
    this.literal = literal;
  }

  public SqlLiteral getLiteral() {
    return literal;
  }

  @Override
  public SqlBoundDynamicParam clone(SqlParserPos pos) {
    return new SqlBoundDynamicParam(getIndex(), pos, literal);
  }
}
