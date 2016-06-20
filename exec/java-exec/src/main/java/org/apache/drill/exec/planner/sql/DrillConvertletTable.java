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

import java.util.Map;

import com.google.common.collect.ImmutableMap;
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
  public static SqlRexConvertletTable INSTANCE = new DrillConvertletTable();

  private static final Map<SqlOperator, SqlRexConvertlet> map =
      ImmutableMap.<SqlOperator, SqlRexConvertlet>builder()
          .put(SqlStdOperatorTable.EXTRACT, DrillExtractConvertlet.INSTANCE)
          .put(SqlStdOperatorTable.IS_DISTINCT_FROM, DrillDistinctFromConvertlet.INSTANCE)
          .put(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, DrillDistinctFromConvertlet.INSTANCE)
          .put(SqlStdOperatorTable.AVG, new DrillAvgVarianceConvertlet(SqlAvgAggFunction.Subtype.AVG))
          .put(SqlStdOperatorTable.STDDEV_POP, new DrillAvgVarianceConvertlet(SqlAvgAggFunction.Subtype.STDDEV_POP))
          .put(SqlStdOperatorTable.STDDEV_SAMP, new DrillAvgVarianceConvertlet(SqlAvgAggFunction.Subtype.STDDEV_SAMP))
          .put(SqlStdOperatorTable.VAR_POP, new DrillAvgVarianceConvertlet(SqlAvgAggFunction.Subtype.VAR_POP))
          .put(SqlStdOperatorTable.VAR_SAMP, new DrillAvgVarianceConvertlet(SqlAvgAggFunction.Subtype.VAR_SAMP))
      .build();

  private DrillConvertletTable() {}

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

}
