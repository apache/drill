/*
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

package org.apache.drill.exec.physical.unit;

import com.google.common.collect.Lists;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.physical.config.Filter;
import org.apache.drill.exec.physical.config.UnionAll;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

/**
 * This class contains examples to show how to use MiniPlanTestBuilder to test a
 * specific plan fragment (MiniPlan). Each testcase requires 1) a RecordBatch,
 * built from PopBuilder/ScanBuilder, 2)an expected schema and base line values,
 * or 3) indicating no batch is expected.
 */
public class TestMiniPlan extends MiniPlanUnitTestBase {

  protected static DrillFileSystem fs;

  @BeforeClass
  public static void initFS() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
    fs = new DrillFileSystem(conf);
  }

  @Test
  @Ignore("DRILL-5464: A bug in JsonRecordReader handling empty file")
  public void testEmptyJsonInput() throws Exception {
    String emptyFile = FileUtils.getResourceAsFile("/project/pushdown/empty.json").toURI().toString();

    RecordBatch scanBatch = new JsonScanBuilder()
        .fileSystem(fs)
        .inputPaths(Lists.newArrayList(emptyFile))
        .build();

    new MiniPlanTestBuilder()
        .root(scanBatch)
        .expectZeroBatch(true)
        .go();
  }

  @Test
  public void testSimpleParquetScan() throws Exception {
    String file = FileUtils.getResourceAsFile("/tpchmulti/region/01.parquet").toURI().toString();

    RecordBatch scanBatch = new ParquetScanBuilder()
        .fileSystem(fs)
        .columnsToRead("R_REGIONKEY")
        .inputPaths(Lists.newArrayList(file))
        .build();

    BatchSchema expectedSchema = new SchemaBuilder()
        .add("R_REGIONKEY", TypeProtos.MinorType.BIGINT)
        .build();

    new MiniPlanTestBuilder()
        .root(scanBatch)
        .expectedSchema(expectedSchema)
        .baselineValues(0L)
        .baselineValues(1L)
        .go();
  }

  @Test
  public void testSimpleJson() throws Exception {
    List<String> jsonBatches = Lists.newArrayList(
        "{\"a\":100}"
    );

    RecordBatch scanBatch = new JsonScanBuilder()
        .jsonBatches(jsonBatches)
        .build();

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", TypeProtos.MinorType.BIGINT)
        .build();

    new MiniPlanTestBuilder()
        .root(scanBatch)
        .expectedSchema(expectedSchema)
        .baselineValues(100L)
        .go();
  }

  @Test
  public void testUnionFilter() throws Exception {
    List<String> leftJsonBatches = Lists.newArrayList(
        "[{\"a\": 5, \"b\" : 1 }]",
        "[{\"a\": 5, \"b\" : 5},{\"a\": 3, \"b\" : 8}]",
        "[{\"a\": 40, \"b\" : 3},{\"a\": 13, \"b\" : 100}]");

    List<String> rightJsonBatches = Lists.newArrayList(
        "[{\"a\": 5, \"b\" : 10 }]",
        "[{\"a\": 50, \"b\" : 100}]");

    RecordBatch batch = new PopBuilder()
        .physicalOperator(new UnionAll(Collections.EMPTY_LIST)) // Children list is provided through RecordBatch
        .addInputAsChild()
          .physicalOperator(new Filter(null, parseExpr("a=5"), 1.0f))
          .addJsonScanAsChild()
            .jsonBatches(leftJsonBatches)
            .columnsToRead("a", "b")
            .buildAddAsInput()
          .buildAddAsInput()
        .addInputAsChild()
          .physicalOperator(new Filter(null, parseExpr("a=50"), 1.0f))
          .addJsonScanAsChild()
            .jsonBatches(rightJsonBatches)
            .columnsToRead("a", "b")
            .buildAddAsInput()
          .buildAddAsInput()
        .build();

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", TypeProtos.MinorType.BIGINT)
        .addNullable("b", TypeProtos.MinorType.BIGINT)
        .withSVMode(BatchSchema.SelectionVectorMode.NONE)
        .build();

    new MiniPlanTestBuilder()
        .root(batch)
        .expectedSchema(expectedSchema)
        .baselineValues(5l, 1l)
        .baselineValues(5l, 5l)
        .baselineValues(50l, 100l)
        .go();
  }

  @Test
  @Ignore ("DRILL-5327: A bug in UnionAll handling empty inputs from both sides")
  public void testUnionFilterAll() throws Exception {
    List<String> leftJsonBatches = Lists.newArrayList(
        "[{\"a\": 5, \"b\" : 1 }]");

    List<String> rightJsonBatches = Lists.newArrayList(
        "[{\"a\": 50, \"b\" : 10 }]");

    RecordBatch leftScan = new JsonScanBuilder()
        .jsonBatches(leftJsonBatches)
        .columnsToRead("a", "b")
        .build();

    RecordBatch leftFilter = new PopBuilder()
        .physicalOperator(new Filter(null, parseExpr("a < 0"), 1.0f))
        .addInput(leftScan)
        .build();

    RecordBatch rightScan = new JsonScanBuilder()
        .jsonBatches(rightJsonBatches)
        .columnsToRead("a", "b")
        .build();

    RecordBatch rightFilter = new PopBuilder()
        .physicalOperator(new Filter(null, parseExpr("a < 0"), 1.0f))
        .addInput(rightScan)
        .build();

    RecordBatch batch = new PopBuilder()
        .physicalOperator(new UnionAll(Collections.EMPTY_LIST)) // Children list is provided through RecordBatch
        .addInput(leftFilter)
        .addInput(rightFilter)
        .build();

    BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("a", TypeProtos.MinorType.BIGINT)
        .addNullable("b", TypeProtos.MinorType.BIGINT)
        .withSVMode(BatchSchema.SelectionVectorMode.NONE)
        .build();

    new MiniPlanTestBuilder()
        .root(batch)
        .expectedSchema(expectedSchema)
        .go();
  }

}
