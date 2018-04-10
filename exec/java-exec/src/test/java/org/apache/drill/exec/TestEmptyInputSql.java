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

package org.apache.drill.exec;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.test.BaseTestQuery;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.apache.drill.categories.UnlikelyTest;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.BatchSchema;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Paths;
import java.util.List;

@Category(UnlikelyTest.class)
public class TestEmptyInputSql extends BaseTestQuery {

  private static final String SINGLE_EMPTY_JSON = "/scan/emptyInput/emptyJson/empty.json";
  private static final String SINGLE_EMPTY_CSVH = "/scan/emptyInput/emptyCsvH/empty.csvh";
  private static final String SINGLE_EMPTY_CSV = "/scan/emptyInput/emptyCsv/empty.csv";
  private static final String EMPTY_DIR_NAME = "empty_directory";

  @BeforeClass
  public static void setupTestFiles() {
    dirTestWatcher.makeTestTmpSubDir(Paths.get(EMPTY_DIR_NAME));
  }

  /**
   * Test with query against an empty file. Select clause has regular column reference, and an expression.
   *
   * regular column "key" is assigned with nullable-int
   * expression "key + 100" is materialized with nullable-int as output type.
   */
  @Test
  public void testQueryEmptyJson() throws Exception {
    final BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("key", TypeProtos.MinorType.INT)
        .addNullable("key2", TypeProtos.MinorType.INT)
        .build();

    testBuilder()
        .sqlQuery("select key, key + 100 as key2 from cp.`%s`", SINGLE_EMPTY_JSON)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  /**
   * Test with query against an empty file. Select clause has one or more *
   * star column is expanded into an empty list.
   * @throws Exception
   */
  @Test
  public void testQueryStarColEmptyJson() throws Exception {
    final BatchSchema expectedSchema = new SchemaBuilder()
        .build();

    testBuilder()
        .sqlQuery("select * from cp.`%s` ", SINGLE_EMPTY_JSON)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();

    testBuilder()
        .sqlQuery("select *, * from cp.`%s` ", SINGLE_EMPTY_JSON)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  /**
   * Test with query against an empty file. Select clause has one or more qualified *
   * star column is expanded into an empty list.
   * @throws Exception
   */
  @Test
  public void testQueryQualifiedStarColEmptyJson() throws Exception {
    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();

    testBuilder()
        .sqlQuery("select foo.* from cp.`%s` as foo", SINGLE_EMPTY_JSON)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();

    testBuilder()
        .sqlQuery("select foo.*, foo.* from cp.`%s` as foo", SINGLE_EMPTY_JSON)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();

  }

  @Test
  public void testQueryMapArrayEmptyJson() throws Exception {
    final BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("col1", TypeProtos.MinorType.INT)
        .addNullable("col2", TypeProtos.MinorType.INT)
        .addNullable("col3", TypeProtos.MinorType.INT)
        .build();

    testBuilder()
        .sqlQuery("select foo.a.b as col1, foo.columns[2] as col2, foo.bar.columns[3] as col3 from cp.`%s` as foo", SINGLE_EMPTY_JSON)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  /**
   * Test with query against an empty file. Select clause has three expressions.
   * 1.0 + 100.0 as constant expression, is resolved to required FLOAT8
   * cast(100 as varchar(100) is resolved to required varchar(100)
   * cast(columns as varchar(100)) is resolved to nullable varchar(100).
   */
  @Test
  public void testQueryConstExprEmptyJson() throws Exception {
    final BatchSchema expectedSchema = new SchemaBuilder()
        .add("key", TypeProtos.MinorType.FLOAT8)
        .add("name", TypeProtos.MinorType.VARCHAR, 100)
        .addNullable("name2", TypeProtos.MinorType.VARCHAR, 100)
        .build();

    testBuilder()
        .sqlQuery("select 1.0 + 100.0 as key, "
          + " cast(100 as varchar(100)) as name, "
          + " cast(columns as varchar(100)) as name2 "
          + " from cp.`%s` ", SINGLE_EMPTY_JSON)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  /**
   * Test select * against empty csv with empty header. * is expanded into empty list of fields.
   * @throws Exception
   */
  @Test
  public void testQueryEmptyCsvH() throws Exception {
    final BatchSchema expectedSchema = new SchemaBuilder()
        .build();

    testBuilder()
        .sqlQuery("select * from cp.`%s`", SINGLE_EMPTY_CSVH)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  /**
   * Test select * against empty csv file. * is exapnede into "columns : repeated-varchar",
   * which is the default column from reading a csv file.
   * @throws Exception
   */
  @Test
  public void testQueryEmptyCsv() throws Exception {
    final BatchSchema expectedSchema = new SchemaBuilder()
        .addArray("columns", TypeProtos.MinorType.VARCHAR)
        .build();

    testBuilder()
        .sqlQuery("select * from cp.`%s`", SINGLE_EMPTY_CSV)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testEmptyDirectory() throws Exception {
    final BatchSchema expectedSchema = new SchemaBuilder().build();

    testBuilder()
        .sqlQuery("select * from dfs.tmp.`%s`", EMPTY_DIR_NAME)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testEmptyDirectoryAndFieldInQuery() throws Exception {
    final List<Pair<SchemaPath, TypeProtos.MajorType>> expectedSchema = Lists.newArrayList();
    final TypeProtos.MajorType majorType = TypeProtos.MajorType.newBuilder()
        .setMinorType(TypeProtos.MinorType.INT) // field "key" is absent in schemaless table
        .setMode(TypeProtos.DataMode.OPTIONAL)
        .build();
    expectedSchema.add(Pair.of(SchemaPath.getSimplePath("key"), majorType));

    testBuilder()
        .sqlQuery("select key from dfs.tmp.`%s`", EMPTY_DIR_NAME)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }



}
