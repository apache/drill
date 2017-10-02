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
package org.apache.drill.exec.store;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.util.Text;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class TestImplicitFileColumns extends BaseTestQuery {

  public static final String MAIN = "main";
  public static final String NESTED = "nested";
  public static final String CSV = "csv";
  public final String JSON_TBL = "/scan/jsonTbl"; // 1990/1.json : {id:100, name: "John"}, 1991/2.json : {id: 1000, name : "Joe"}
  public final String PARQUET_TBL = "/multilevel/parquet/";  // 1990/Q1/orders_1990_q1.parquet, ...
  public final String CSV_TBL = "/multilevel/csv";  // 1990/Q1/orders_1990_q1.csv, ..

  private static final JsonStringArrayList<Text> mainColumnValues = new JsonStringArrayList<Text>() {{
    add(new Text(MAIN));
  }};
  private static final JsonStringArrayList<Text> nestedColumnValues = new JsonStringArrayList<Text>() {{
    add(new Text(NESTED));
  }};

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  private File mainFile;
  private File nestedFolder;
  private File nestedFile;

  @Before
  public void setup() throws Exception {
    mainFile = testFolder.newFile(MAIN + "." + CSV);
    Files.write(MAIN, mainFile, Charsets.UTF_8);
    nestedFolder = testFolder.newFolder(NESTED);
    nestedFile = new File(nestedFolder, NESTED + "." + CSV);
    Files.write(NESTED, nestedFile, Charsets.UTF_8);
  }

  @Test
  public void testImplicitColumns() throws Exception {
    testBuilder()
        .sqlQuery("select *, filename, suffix, fqn, filepath from dfs.`%s` order by filename", testFolder.getRoot().getPath())
        .ordered()
        .baselineColumns("columns", "dir0", "filename", "suffix", "fqn", "filepath")
        .baselineValues(mainColumnValues, null, mainFile.getName(), CSV, new Path(mainFile.getPath()).toString(), new Path(mainFile.getParent()).toString())
        .baselineValues(nestedColumnValues, NESTED, nestedFile.getName(), CSV, new Path(nestedFile.getPath()).toString(), new Path(nestedFile.getParent()).toString())
        .go();
  }

  @Test
  public void testImplicitColumnInWhereClause() throws Exception {
    testBuilder()
        .sqlQuery("select * from dfs.`%s` where filename = '%s'", nestedFolder.getPath(), nestedFile.getName())
        .unOrdered()
        .baselineColumns("columns")
        .baselineValues(nestedColumnValues)
        .go();
  }

  @Test
  public void testImplicitColumnAlone() throws Exception {
    testBuilder()
        .sqlQuery("select filename from dfs.`%s`", nestedFolder.getPath())
        .unOrdered()
        .baselineColumns("filename")
        .baselineValues(nestedFile.getName())
        .go();
  }

  @Test
  public void testImplicitColumnWithTableColumns() throws Exception {
    testBuilder()
        .sqlQuery("select columns, filename from dfs.`%s`", nestedFolder.getPath())
        .unOrdered()
        .baselineColumns("columns", "filename")
        .baselineValues(nestedColumnValues, nestedFile.getName())
        .go();
  }

  @Test
  public void testCountStarWithImplicitColumnsInWhereClause() throws Exception {
    testBuilder()
        .sqlQuery("select count(*) as cnt from dfs.`%s` where filename = '%s'", nestedFolder.getPath(), nestedFile.getName())
        .unOrdered()
        .baselineColumns("cnt")
        .baselineValues(1L)
        .go();
  }

  @Test
  public void testImplicitAndPartitionColumnsInSelectClause() throws Exception {
    testBuilder()
        .sqlQuery("select dir0, filename from dfs.`%s` order by filename", testFolder.getRoot().getPath()).ordered()
        .baselineColumns("dir0", "filename")
        .baselineValues(null, mainFile.getName())
        .baselineValues(NESTED, nestedFile.getName())
        .go();
  }

  @Test
  public void testImplicitColumnsForParquet() throws Exception {
    testBuilder()
        .sqlQuery("select filename, suffix from cp.`tpch/region.parquet` limit 1")
        .unOrdered()
        .baselineColumns("filename", "suffix")
        .baselineValues("region.parquet", "parquet")
        .go();
  }

  @Test // DRILL-4733
  public void testMultilevelParquetWithSchemaChange() throws Exception {
    try {
      test("alter session set `planner.enable_decimal_data_type` = true");
      testBuilder()
          .sqlQuery(String.format("select max(dir0) as max_dir from dfs_test.`%s/src/test/resources/multilevel/parquetWithSchemaChange`",
              TestTools.getWorkingPath()))
          .unOrdered()
          .baselineColumns("max_dir")
          .baselineValues("voter50")
          .go();
    } finally {
      test("alter session set `planner.enable_decimal_data_type` = false");
    }
  }

  @Test
  public void testStarColumnJson() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile(JSON_TBL).toURI().toString();
    final String query1 = String.format("select * from dfs_test.`%s` ", rootEmpty);

    final BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("dir0", TypeProtos.MinorType.VARCHAR)
        .addNullable("id", TypeProtos.MinorType.BIGINT)
        .addNullable("name", TypeProtos.MinorType.VARCHAR)
        .build();

    testBuilder()
        .sqlQuery(query1)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testStarColumnParquet() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile(PARQUET_TBL).toURI().toString();
    final String query1 = String.format("select * from dfs_test.`%s` ", rootEmpty);

    final BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("dir0", TypeProtos.MinorType.VARCHAR)
        .addNullable("dir1", TypeProtos.MinorType.VARCHAR)
        .add("o_orderkey", TypeProtos.MinorType.INT)
        .add("o_custkey", TypeProtos.MinorType.INT)
        .add("o_orderstatus", TypeProtos.MinorType.VARCHAR)
        .add("o_totalprice", TypeProtos.MinorType.FLOAT8)
        .add("o_orderdate", TypeProtos.MinorType.DATE)
        .add("o_orderpriority", TypeProtos.MinorType.VARCHAR)
        .add("o_clerk", TypeProtos.MinorType.VARCHAR)
        .add("o_shippriority", TypeProtos.MinorType.INT)
        .add("o_comment", TypeProtos.MinorType.VARCHAR)
        .build();

    testBuilder()
        .sqlQuery(query1)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

  @Test
  public void testStarColumnCsv() throws Exception {
    final String rootEmpty = FileUtils.getResourceAsFile(CSV_TBL).toURI().toString();
    final String query1 = String.format("select * from dfs_test.`%s` ", rootEmpty);

    final BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("dir0", TypeProtos.MinorType.VARCHAR)
        .addNullable("dir1", TypeProtos.MinorType.VARCHAR)
        .addArray("columns", TypeProtos.MinorType.VARCHAR)
        .build();

    testBuilder()
        .sqlQuery(query1)
        .schemaBaseLine(expectedSchema)
        .build()
        .run();
  }

}
