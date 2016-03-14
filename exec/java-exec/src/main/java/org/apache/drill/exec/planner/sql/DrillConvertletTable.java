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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.drill.exec.planner.sql.parser.DrillCalciteWrapperUtility;

public class DrillConvertletTable implements SqlRexConvertletTable{

  public static HashMap<SqlOperator, SqlRexConvertlet> map = new HashMap<>();

  public static SqlRexConvertletTable INSTANCE = new DrillConvertletTable();

  static {
    // Use custom convertlet for extract function
    map.put(SqlStdOperatorTable.EXTRACT, DrillExtractConvertlet.INSTANCE);
    map.put(SqlStdOperatorTable.AVG, new DrillAvgVarianceConvertlet(SqlAvgAggFunction.Subtype.AVG));
    map.put(SqlStdOperatorTable.STDDEV_POP, new DrillAvgVarianceConvertlet(SqlAvgAggFunction.Subtype.STDDEV_POP));
    map.put(SqlStdOperatorTable.STDDEV_SAMP, new DrillAvgVarianceConvertlet(SqlAvgAggFunction.Subtype.STDDEV_SAMP));
    map.put(SqlStdOperatorTable.VAR_POP, new DrillAvgVarianceConvertlet(SqlAvgAggFunction.Subtype.VAR_POP));
    map.put(SqlStdOperatorTable.VAR_SAMP, new DrillAvgVarianceConvertlet(SqlAvgAggFunction.Subtype.VAR_SAMP));
  }

  /*
   * Lookup the hash table to see if we have a custom convertlet for a given
   * operator, if we don't use StandardConvertletTable.
   */
  @Override
  public SqlRexConvertlet get(SqlCall call) {
    SqlRexConvertlet convertlet;
    if(call.getOperator() instanceof DrillCalciteSqlWrapper) {
      final SqlOperator wrapper = call.getOperator();
      final SqlOperator wrapped = DrillCalciteWrapperUtility.extractSqlOperatorFromWrapper(call.getOperator());
      if ((convertlet = map.get(wrapped)) != null) {
        return convertlet;
      }

      ((SqlBasicCall) call).setOperator(wrapped);
      SqlRexConvertlet sqlRexConvertlet = StandardConvertletTable.INSTANCE.get(call);
      ((SqlBasicCall) call).setOperator(wrapper);
      return sqlRexConvertlet;
    }

    if ((convertlet = map.get(call.getOperator())) != null) {
      return convertlet;
    }

    return StandardConvertletTable.INSTANCE.get(call);
  }

  private DrillConvertletTable() {
  }
}
