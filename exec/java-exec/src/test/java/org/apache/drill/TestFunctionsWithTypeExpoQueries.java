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
package org.apache.drill;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TestFunctionsWithTypeExpoQueries extends BaseTestQuery {
  @Test
  public void testConcatWithMoreThanTwoArgs() throws Exception {
    final String query = "select concat(r_name, r_name, r_name) as col \n" +
        "from cp.`tpch/region.parquet` limit 0";

    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
            .setMinorType(TypeProtos.MinorType.VARCHAR)
            .setMode(TypeProtos.DataMode.REQUIRED)
            .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testTrimOnlyOneArg() throws Exception {
    final String query1 = "SELECT ltrim('drill') as col FROM (VALUES(1)) limit 0";
    final String query2 = "SELECT rtrim('drill') as col FROM (VALUES(1)) limit 0";
    final String query3 = "SELECT btrim('drill') as col FROM (VALUES(1)) limit 0";

    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.VARCHAR)
        .setMode(TypeProtos.DataMode.REQUIRED)
        .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query1)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query2)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();

    testBuilder()
        .sqlQuery(query3)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testExtract() throws Exception {
    final String query = "select extract(second from time '02:30:45.100') as col \n" +
        "from cp.`employee.json` limit 0";
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.FLOAT8)
        .setMode(TypeProtos.DataMode.OPTIONAL)
        .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void tesIsNull() throws Exception {
    final String query = "select r_name is null as col from cp.`tpch/region.parquet` limit 0";
    List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.BIT)
        .setMode(TypeProtos.DataMode.OPTIONAL)
        .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("col"), majorType));

    testBuilder()
        .sqlQuery(query)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }
}
