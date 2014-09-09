/**
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
package org.apache.drill.exec.planner.sql;

import java.util.HashMap;

import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql2rel.SqlRexConvertlet;
import org.eigenbase.sql2rel.SqlRexConvertletTable;
import org.eigenbase.sql2rel.StandardConvertletTable;

public class DrillConvertletTable implements SqlRexConvertletTable{

  public static HashMap<SqlOperator, SqlRexConvertlet> map = new HashMap<>();

  static {
    // Use custom convertlet for extract function
    map.put(SqlStdOperatorTable.EXTRACT, DrillExtractConvertlet.INSTANCE);
  }

  /*
   * Lookup the hash table to see if we have a custom convertlet for a given
   * operator, if we don't use StandardConvertletTable.
   */
  @Override
  public SqlRexConvertlet get(SqlCall call) {

    SqlRexConvertlet convertlet;

    if ((convertlet = map.get(call.getOperator())) != null) {
      return convertlet;
    }

    return StandardConvertletTable.INSTANCE.get(call);
  }
}
