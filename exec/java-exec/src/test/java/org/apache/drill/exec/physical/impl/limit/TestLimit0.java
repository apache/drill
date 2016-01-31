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
package org.apache.drill.exec.physical.impl.limit;

import com.google.common.collect.ImmutableList;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.PlanTestBase;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.drill.TestBuilder.mapOf;

public class TestLimit0 extends BaseTestQuery {

  @Test
  public void simpleLimit0() throws Exception {
    List<MajorType> expectedTypes = ImmutableList.of(
            Types.optional(MinorType.VARCHAR),
            Types.optional(MinorType.BIGINT),
            Types.optional(MinorType.DATE),
            Types.optional(MinorType.TIME),
            Types.optional(MinorType.TIMESTAMP),
            Types.optional(MinorType.INTERVAL), // I think these are incorrect, losing DAY/YEAR
            Types.optional(MinorType.INTERVAL));


    List<QueryDataBatch> results = testSqlWithResults("SELECT * FROM (" +
                "SELECT * FROM (VALUES ('varchar', 1, DATE '2008-2-23', TIME '12:23:34', TIMESTAMP '2008-2-23 12:23:34.456', " +
                "INTERVAL '1' YEAR, INTERVAL '2' DAY), ('varchar', 1, DATE '2008-2-23', TIME '12:23:34', " +
                "TIMESTAMP '2008-2-23 12:23:34.456', INTERVAL '1' YEAR, INTERVAL '2' DAY)) AS " +
                "Example(myVarChar, myInt, myDate, myTime, myTimestamp, int1, int2)) T LIMIT 0");
    Assert.assertEquals(0, results.get(0).getHeader().getRowCount());
    List<SerializedField> fields = results.get(0).getHeader().getDef().getFieldList();
    Assert.assertEquals(expectedTypes.size(), fields.size());
    for (int i = 0; i < fields.size(); i++) {
      Assert.assertEquals(expectedTypes.get(i), fields.get(i).getMajorType());
    }
  }

  @Test
  public void simpleLimit0Plan() throws Exception {
    PlanTestBase.testPlanMatchingPatterns("SELECT * FROM (" +
        "SELECT * FROM (VALUES (1, 1.0, DATE '2008-2-23', TIME '12:23:34', TIMESTAMP '2008-2-23 12:23:34.456', " +
        "INTERVAL '1' YEAR, INTERVAL '2' DAY), (1, 1.0, DATE '2008-2-23', TIME '12:23:34', " +
        "TIMESTAMP '2008-2-23 12:23:34.456', INTERVAL '1' YEAR, INTERVAL '2' DAY)) AS " +
        "Example(myInt, myFloat, myDate, myTime, myTimestamp, int1, int2)) T LIMIT 0",
        new String[] {
            ".*Limit.*\n" +
                ".*Values.*"
        },
        new String[] {});
  }

  @Test
  public void countDistinctLimit0() throws Exception {
    testBuilder()
        .sqlQuery("SELECT * FROM " +
        "(SELECT CAST(COUNT(employee_id) AS BIGINT) as c1, " +
        "CAST(SUM(employee_id) AS INT) as s1, " +
        "CAST(COUNT(DISTINCT employee_id) AS BIGINT) as c2 " +
        "FROM cp.`employee.json`) " +
        "T LIMIT 0")
        .baselineColumns("c1", "s1", "c2")
        .expectsEmptyResultSet()
        .go();
  }

  @Test
  public void countDistinctLimit0Plan() throws Exception {
    PlanTestBase.testPlanMatchingPatterns("SELECT * FROM " +
            "(SELECT CAST(COUNT(employee_id) AS BIGINT) as c1, " +
            "CAST(SUM(employee_id) AS INT) as s1, " +
            "CAST(COUNT(DISTINCT employee_id) AS BIGINT) as c2 " +
            "FROM cp.`employee.json`) " +
            "T LIMIT 0",
        new String[] {
            ".*Limit.*\n" +
                ".*Values.*"
        },
        new String[] {});
  }
}
