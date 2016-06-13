/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.work.prepare;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType;
import org.apache.drill.exec.proto.UserProtos.ColumnSearchability;
import org.apache.drill.exec.proto.UserProtos.ColumnUpdatability;
import org.apache.drill.exec.proto.UserProtos.CreatePreparedStatementResp;
import org.apache.drill.exec.proto.UserProtos.PreparedStatement;
import org.apache.drill.exec.proto.UserProtos.RequestStatus;
import org.apache.drill.exec.proto.UserProtos.ResultColumnMetadata;
import org.apache.drill.exec.store.ischema.InfoSchemaConstants;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import java.sql.Date;
import java.util.List;

/**
 * Tests for creating and executing prepared statements.
 */
public class TestPreparedStatementProvider extends BaseTestQuery {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestPreparedStatementProvider.class);

  /**
   * Simple query.
   * @throws Exception
   */
  @Test
  public void simple() throws Exception {
    String query = "SELECT * FROM cp.`region.json` ORDER BY region_id LIMIT 1";
    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("region_id", "BIGINT", true, 0, 0, true, Long.class.getName()),
        new ExpectedColumnResult("sales_city", "CHARACTER VARYING", true, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_state_province", "CHARACTER VARYING", true, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_district", "CHARACTER VARYING", true, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_region", "CHARACTER VARYING", true, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_country", "CHARACTER VARYING", true, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("sales_district_id", "BIGINT", true, 0, 0, true, Long.class.getName())
    );

    verifyMetadata(expMetadata, preparedStatement.getColumnsList());

    testBuilder()
        .unOrdered()
        .preparedStatement(preparedStatement.getServerHandle())
        .baselineColumns("region_id", "sales_city", "sales_state_province", "sales_district",
            "sales_region", "sales_country", "sales_district_id")
        .baselineValues(0L, "None", "None", "No District", "No Region", "No Country", 0L)
        .go();
  }

  /**
   * Create a prepared statement for a query that has GROUP BY clause in it
   */
  @Test
  public void groupByQuery() throws Exception {
    String query = "SELECT sales_city, count(*) as cnt FROM cp.`region.json` " +
        "GROUP BY sales_city ORDER BY sales_city DESC LIMIT 1";
    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("sales_city", "CHARACTER VARYING", true, 65536, 0, false, String.class.getName()),
        new ExpectedColumnResult("cnt", "BIGINT", false, 0, 0, true, Long.class.getName())
    );

    verifyMetadata(expMetadata, preparedStatement.getColumnsList());

    testBuilder()
        .unOrdered()
        .preparedStatement(preparedStatement.getServerHandle())
        .baselineColumns("sales_city", "cnt")
        .baselineValues("Yakima", 1L)
        .go();
  }

  /**
   * Create a prepared statement for a query that joins two tables and has ORDER BY clause.
   */
  @Test
  public void joinOrderByQuery() throws Exception {
    String query = "SELECT l.l_quantity, l.l_shipdate, o.o_custkey FROM cp.`tpch/lineitem.parquet` l JOIN cp.`tpch/orders.parquet` o " +
        "ON l.l_orderkey = o.o_orderkey LIMIT 2";

    PreparedStatement preparedStatement = createPrepareStmt(query, false, null);

    List<ExpectedColumnResult> expMetadata = ImmutableList.of(
        new ExpectedColumnResult("l_quantity", "DOUBLE", false, 0, 0, true, Double.class.getName()),
        new ExpectedColumnResult("l_shipdate", "DATE", false, 0, 0, false, Date.class.getName()),
        new ExpectedColumnResult("o_custkey", "INTEGER", false, 0, 0, true, Integer.class.getName())
    );

    verifyMetadata(expMetadata, preparedStatement.getColumnsList());
  }

  /**
   * Pass an invalid query to the create prepare statement request and expect a parser failure.
   * @throws Exception
   */
  @Test
  public void invalidQueryParserError() throws Exception {
    createPrepareStmt("BLAH BLAH", true, ErrorType.PARSE);
  }

  /**
   * Pass an invalid query to the create prepare statement request and expect a validation failure.
   * @throws Exception
   */
  @Test
  public void invalidQueryValidationError() throws Exception {
    createPrepareStmt("SELECT * sdflkgdh", true, ErrorType.PARSE /** Drill returns incorrect error for parse error*/);
  }

  /* Helper method which creates a prepared statement for given query. */
  private static PreparedStatement createPrepareStmt(String query, boolean expectFailure, ErrorType errorType) throws Exception {
    CreatePreparedStatementResp resp = client.createPreparedStatement(query).get();

    assertEquals(expectFailure ? RequestStatus.FAILED : RequestStatus.OK, resp.getStatus());

    if (expectFailure) {
      assertEquals(errorType, resp.getError().getErrorType());
    } else {
      logger.error("Prepared statement creation failed: {}", resp.getError().getMessage());
    }

    return resp.getPreparedStatement();
  }

  private static class ExpectedColumnResult {
    final String columnName;
    final String type;
    final boolean nullable;
    final int precision;
    final int scale;
    final boolean signed;
    final String className;

    ExpectedColumnResult(String columnName, String type, boolean nullable, int precision, int scale, boolean signed,
        String className) {
      this.columnName = columnName;
      this.type = type;
      this.nullable = nullable;
      this.precision = precision;
      this.scale = scale;
      this.signed = signed;
      this.className = className;
    }

    boolean isEqualsTo(ResultColumnMetadata result) {
      return
          result.getCatalogName().equals(InfoSchemaConstants.IS_CATALOG_NAME) &&
          result.getSchemaName().isEmpty() &&
          result.getTableName().isEmpty() &&
          result.getColumnName().equals(columnName) &&
          result.getLabel().equals(columnName) &&
          result.getDataType().equals(type) &&
          result.getIsNullable() == nullable &&
          result.getPrecision() == precision &&
          result.getScale() == scale &&
          result.getSigned() == signed &&
          result.getDisplaySize() == 10 &&
          result.getClassName().equals(className) &&
          result.getSearchability() == ColumnSearchability.ALL &&
          result.getAutoIncrement() == false &&
          result.getCaseSensitivity() == false &&
          result.getUpdatability() == ColumnUpdatability.READ_ONLY &&
          result.getIsAliased() == true &&
          result.getIsCurrency() == false;
    }

    @Override
    public String toString() {
      return "ExpectedColumnResult[" +
          "columnName='" + columnName + '\'' +
          ", type='" + type + '\'' +
          ", nullable=" + nullable +
          ", precision=" + precision +
          ", scale=" + scale +
          ", signed=" + signed +
          ", className='" + className + '\'' +
          ']';
    }
  }

  private static void verifyMetadata(List<ExpectedColumnResult> expMetadata,
      List<ResultColumnMetadata> actMetadata) {
    assertEquals(expMetadata.size(), actMetadata.size());

    for(ExpectedColumnResult exp : expMetadata) {
      boolean found = false;
      for(ResultColumnMetadata act : actMetadata) {
        found = exp.isEqualsTo(act);
        if (found) {
          break;
        }
      }
      assertTrue("Failed to find the expected column metadata: " + exp, found);
    }
  }
}
