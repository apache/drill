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

package org.apache.calcite.sql.validate;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.DynamicSchema;
import org.apache.drill.exec.rpc.user.UserSession;

public class DrillSqlValidatorUtil {

  /**
   * Finds and returns {@link CalciteSchema} nested to the given rootSchema
   * with specified schemaPath.
   *
   * <p>Uses the case-sensitivity policy of specified nameMatcher.
   *
   * <p>If not found, returns null.
   *
   * @param rootSchema root schema
   * @param schemaPath full schema path of required schema
   * @param nameMatcher name matcher
   * @param session Drill user session
   *
   * @return CalciteSchema that corresponds specified schemaPath
   */
  public static CalciteSchema getSchema(CalciteSchema rootSchema,
                                        Iterable<String> schemaPath, SqlNameMatcher nameMatcher, UserSession session) {
    CalciteSchema schema = rootSchema;
    DrillSqlValidatorUtil.setUserSession(schema, session);
    for (String schemaName : schemaPath) {
      if (schema == rootSchema
        && nameMatcher.matches(schemaName, schema.getName())) {
        DrillSqlValidatorUtil.setUserSession(schema, session);
        continue;
      }
      schema = schema.getSubSchema(schemaName,
        nameMatcher.isCaseSensitive());
      if (schema == null) {
        return null;
      }
    }
    return schema;
  }

  public static void setUserSession(CalciteSchema schema, UserSession session) {
    if (schema == null) {
      return;
    }
    if (schema instanceof DynamicSchema) {
      ((DynamicSchema)schema).setSession(session);
    }
  }
}
