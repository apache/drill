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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;

import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.config.FlattenPOP;
import org.apache.drill.exec.physical.config.MergeJoinPOP;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.util.JsonStringArrayList;
import org.apache.drill.exec.util.JsonStringHashMap;
import org.apache.drill.exec.util.Text;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestOutputBatchSize extends PhysicalOpUnitTestBase {
  private static final long initReservation = AbstractBase.INIT_ALLOCATION;
  private static final long maxAllocation = AbstractBase.MAX_ALLOCATION;
  // Keeping row count below 4096 so we do not produce more than one batch.
  // scanBatch with json reader produces batches of 4k.
  private int numRows = 4000;
  private static final String wideString =
    "b00dUrA0oa2i4ZEHg6zvPXPXlVQYB2BXe8T5gIEtvUDzcN6yUkIqyS07gaAy8k4ac6Bn1cxblsXFnkp8g8hiQkUMJPyl6" +
    "l0jTdsIzQ4PkVCURGGyF0aduGqCXUaKp91gqkRMvLhHhmrHdEb22QN20dXEHSygR7vrb2zZhhfWeJbXRsesuYDqdGig801IAS6VWRIdQtJ6gaRhCdNz";

  /**
   *  Figures out what will be total size of the batches for a given Json input batch.
   */
  private long getExpectedSize(List<String> expectedJsonBatches) throws ExecutionSetupException {
    // Create a dummy scanBatch to figure out the size.
    RecordBatch scanBatch = new ScanBatch(new MockPhysicalOperator(), fragContext, getReaderListForJsonBatches(expectedJsonBatches, fragContext));
    Iterable<VectorAccessible> batches = new BatchIterator(scanBatch);

    long totalSize = 0;
    for (VectorAccessible batch : batches) {
      RecordBatchSizer sizer = new RecordBatchSizer(batch);
      totalSize += sizer.netSize();
    }
    return totalSize;
  }

  @Test
  public void testFlattenFixedWidth() throws Exception {
    PhysicalOperator flatten = new FlattenPOP(null, SchemaPath.getSimplePath("c"));
    mockOpContext(flatten, initReservation, maxAllocation);

    // create input rows like this.
    // "a" : 5, "b" : wideString, "c" : [6,7,8,9]
    List<String> inputJsonBatches = Lists.newArrayList();
    StringBuilder batchString = new StringBuilder();
    batchString.append("[");
    for (int i = 0; i < numRows; i++) {
      batchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : [6, 7, 8, 9]},");
    }
    batchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : [6, 7, 8, 9]}");
    batchString.append("]");
    inputJsonBatches.add(batchString.toString());

    // Figure out what will be approximate total output size out of flatten for input above
    // We will use this sizing information to set output batch size so we can produce desired
    // number of batches that can be verified.

    // output rows will be like this.
    // "a" : 5, "b" : wideString, "c" : 6
    // "a" : 5, "b" : wideString, "c" : 7
    // "a" : 5, "b" : wideString, "c" : 8
    // "a" : 5, "b" : wideString, "c" : 9
    List<String> expectedJsonBatches = Lists.newArrayList();
    StringBuilder expectedBatchString = new StringBuilder();
    expectedBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : 6},");
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : 7},");
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : 8},");
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : 9},");
    }
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : 6},");
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : 7},");
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : 8},");
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : 9}");
    expectedBatchString.append("]");
    expectedJsonBatches.add(expectedBatchString.toString());

    long totalSize = getExpectedSize(expectedJsonBatches);

    // set the output batch size to 1/2 of total size expected.
    // We will get approximately 4 batches because of fragmentation factor of 2 accounted for in flatten.
    fragContext.getOptions().setLocalOption("drill.exec.memory.operator.output_batch_size", totalSize / 2);

    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(flatten)
      .inputDataStreamJson(inputJsonBatches)
      .baselineColumns("a", "b", "c")
      .expectedNumBatches(4)  // verify number of batches
      .expectedBatchSize(totalSize / 2); // verify batch size.

    for (int i = 0; i < numRows + 1; i++) {
      opTestBuilder.baselineValues(5l, wideString, 6l);
      opTestBuilder.baselineValues(5l, wideString, 7l);
      opTestBuilder.baselineValues(5l, wideString, 8l);
      opTestBuilder.baselineValues(5l, wideString, 9l);
    }

    opTestBuilder.go();
  }

  @Test
  public void testFlattenVariableWidth() throws Exception {
    PhysicalOperator flatten = new FlattenPOP(null, SchemaPath.getSimplePath("c"));
    mockOpContext(flatten, initReservation, maxAllocation);

    // create input rows like this.
    // "a" : 5, "b" : wideString, "c" : ["parrot", "hummingbird", "owl", "woodpecker", "peacock"]
    List<String> inputJsonBatches = Lists.newArrayList();
    StringBuilder batchString = new StringBuilder();
    batchString.append("[");
    for (int i = 0; i < numRows; i++) {
      batchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\",\"c\" : [\"parrot\", \"hummingbird\", \"owl\", \"woodpecker\", \"peacock\"]},");
    }
    batchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\",\"c\" : [\"parrot\", \"hummingbird\", \"owl\", \"woodpecker\", \"peacock\"]}");
    batchString.append("]");
    inputJsonBatches.add(batchString.toString());

    // Figure out what will be approximate total output size out of flatten for input above
    // We will use this sizing information to set output batch size so we can produce desired
    // number of batches that can be verified.

    // output rows will be like this.
    // "a" : 5, "b" : wideString, "c" : parrot
    // "a" : 5, "b" : wideString, "c" : hummingbird
    // "a" : 5, "b" : wideString, "c" : owl
    // "a" : 5, "b" : wideString, "c" : woodpecker
    // "a" : 5, "b" : wideString, "c" : peacock
    List<String> expectedJsonBatches = Lists.newArrayList();
    StringBuilder expectedBatchString = new StringBuilder();
    expectedBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : \"parrot\"},");
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : \"hummingbird\"},");
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : \"owl\"},");
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : \"woodpecker\"},");
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : \"peacock\"},");
    }
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : \"parrot\"},");
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : \"hummingbird\"},");
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : \"owl\"},");
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : \"woodpecker\"},");
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : \"peacock\"}");
    expectedBatchString.append("]");
    expectedJsonBatches.add(expectedBatchString.toString());

    long totalSize = getExpectedSize(expectedJsonBatches);

    // set the output batch size to 1/2 of total size expected.
    // We will get approximately 4 batches because of fragmentation factor of 2 accounted for in flatten.
    fragContext.getOptions().setLocalOption("drill.exec.memory.operator.output_batch_size", totalSize / 2);

    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(flatten)
      .inputDataStreamJson(inputJsonBatches)
      .baselineColumns("a", "b", "c")
      .expectedNumBatches(4) // verify number of batches
      .expectedBatchSize(totalSize / 2); // verify batch size.

    for (int i = 0; i < numRows + 1; i++) {
      opTestBuilder.baselineValues(5l, wideString, "parrot");
      opTestBuilder.baselineValues(5l, wideString, "hummingbird");
      opTestBuilder.baselineValues(5l, wideString, "owl");
      opTestBuilder.baselineValues(5l, wideString, "woodpecker");
      opTestBuilder.baselineValues(5l, wideString, "peacock");
    }

    opTestBuilder.go();
  }

  @Test
  public void testFlattenFixedWidthList() throws Exception {
    PhysicalOperator flatten = new FlattenPOP(null, SchemaPath.getSimplePath("c"));
    mockOpContext(flatten, initReservation, maxAllocation);

    // create input rows like this.
    // "a" : 5, "b" : wideString, "c" : [[1,2,3,4], [5,6,7,8]]
    List<String> inputJsonBatches = Lists.newArrayList();
    StringBuilder batchString = new StringBuilder();
    batchString.append("[");
    for (int i = 0; i < numRows; i++) {
      batchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : [" + "[1,2,3,4]," + "[5,6,7,8]" + "]");
      batchString.append("},");
    }
    batchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : [" + "[1,2,3,4]," + "[5,6,7,8]" + "]");
    batchString.append("}]");
    inputJsonBatches.add(batchString.toString());

    // Figure out what will be approximate total output size out of flatten for input above
    // We will use this sizing information to set output batch size so we can produce desired
    // number of batches that can be verified.

    // output rows will be like this.
    // "a" : 5, "b" : wideString, "c" : [1,2,3,4]
    // "a" : 5, "b" : wideString, "c" : [5,6,7,8]
    List<String> expectedJsonBatches = Lists.newArrayList();
    StringBuilder expectedBatchString = new StringBuilder();
    expectedBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : \"[1,2,3,4]\"},");
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : \"[5,6,7,8]\"},");
    }

    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : \"[1,2,3,4]\"},");
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : \"[5,6,7,8]\"}");

    expectedBatchString.append("]");
    expectedJsonBatches.add(expectedBatchString.toString());

    long totalSize = getExpectedSize(expectedJsonBatches);
    // set the output batch size to 1/2 of total size expected.
    // We will get approximately 4 batches because of fragmentation factor of 2 accounted for in flatten.
    fragContext.getOptions().setLocalOption("drill.exec.memory.operator.output_batch_size", totalSize / 2);

    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(flatten)
      .inputDataStreamJson(inputJsonBatches)
      .baselineColumns("a", "b", "c")
      .expectedNumBatches(4) // verify number of batches
      .expectedBatchSize(totalSize);  // verify batch size.

    for (int i = 0; i < numRows + 1; i++) {
      opTestBuilder.baselineValues(5l, wideString, new ArrayList<Long>(Arrays.asList(1L, 2L, 3L, 4L)));
      opTestBuilder.baselineValues(5l, wideString, new ArrayList<Long>(Arrays.asList(5L, 6L, 7L, 8L)));
    }

    opTestBuilder.go();
  }

  @Test
  public void testFlattenVariableWidthList() throws Exception {
    PhysicalOperator flatten = new FlattenPOP(null, SchemaPath.getSimplePath("c"));
    mockOpContext(flatten, initReservation, maxAllocation);
    List<String> inputJsonBatches = Lists.newArrayList();
    StringBuilder batchString = new StringBuilder();

    // create input rows like this.
    // "a" : 5, "b" : wideString, "c" : [["parrot", "hummingbird", "owl", "woodpecker"], ["hawk", "nightingale", "swallow", "peacock"]]
    batchString.append("[");
    for (int i = 0; i < numRows; i++) {
      batchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," +
        "\"c\" : [" + "[\"parrot\", \"hummingbird\", \"owl\", \"woodpecker\"]," + "[\"hawk\",\"nightingale\",\"swallow\",\"peacock\"]" + "]");
      batchString.append("},");
    }
    batchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," +
      "\"c\" : [" + "[\"parrot\", \"hummingbird\", \"owl\", \"woodpecker\"]," + "[\"hawk\",\"nightingale\",\"swallow\",\"peacock\"]" + "]");
    batchString.append("}]");
    inputJsonBatches.add(batchString.toString());

    // Figure out what will be approximate total output size out of flatten for input above
    // We will use this sizing information to set output batch size so we can produce desired
    // number of batches that can be verified.

    // output rows will be like this.
    // "a" : 5, "b" : wideString, "c" : ["parrot", "hummingbird", "owl", "woodpecker"]
    // "a" : 5, "b" : wideString, "c" : ["hawk", "nightingale", "swallow", "peacock"]
    List<String> expectedJsonBatches = Lists.newArrayList();
    StringBuilder expectedBatchString = new StringBuilder();

    expectedBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : [\"parrot\", \"hummingbird\", \"owl\", \"woodpecker\"]},");
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : [\"hawk\", \"nightingale\", \"swallow\", \"peacock\"]},");
    }
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : [\"parrot\", \"hummingbird\", \"owl\", \"woodpecker\"]},");
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : [\"hawk\", \"nightingale\", \"swallow\", \"peacock\"]}");

    expectedBatchString.append("]");
    expectedJsonBatches.add(expectedBatchString.toString());

    long totalSize = getExpectedSize(expectedJsonBatches);
    // set the output batch size to 1/2 of total size expected.
    // We will get approximately 4 batches because of fragmentation factor of 2 accounted for in flatten.
    fragContext.getOptions().setLocalOption("drill.exec.memory.operator.output_batch_size", totalSize / 2);

    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(flatten)
      .inputDataStreamJson(inputJsonBatches)
      .baselineColumns("a", "b", "c")
      .expectedNumBatches(4) // verify number of batches
      .expectedBatchSize(totalSize);  // verify batch size.

    final JsonStringArrayList<Text> birds1 = new JsonStringArrayList<Text>() {{
      add(new Text("parrot"));
      add(new Text("hummingbird"));
      add(new Text("owl"));
      add(new Text("woodpecker"));
    }};

    final JsonStringArrayList<Text> birds2 = new JsonStringArrayList<Text>() {{
      add(new Text("hawk"));
      add(new Text("nightingale"));
      add(new Text("swallow"));
      add(new Text("peacock"));
    }};

    for (int i = 0; i < numRows + 1; i++) {
      opTestBuilder.baselineValues(5l, wideString, birds1);
      opTestBuilder.baselineValues(5l, wideString, birds2);
    }

    opTestBuilder.go();
  }

  @Test
  public void testFlattenMap() throws Exception {
    PhysicalOperator flatten = new FlattenPOP(null, SchemaPath.getSimplePath("c"));
    mockOpContext(flatten, initReservation, maxAllocation);

    // create input rows like this.
    // "a" : 5, "b" : wideString, "c" : [{"trans_id":"t1", amount:100, trans_time:7777777, type:sports}, {"trans_id":"t1", amount:100, trans_time:8888888, type:groceries}]

    List<String> inputJsonBatches = Lists.newArrayList();
    StringBuilder batchString = new StringBuilder();

    batchString.append("[");
    for (int i = 0; i < numRows; i++) {
      batchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," +
        "\"c\" : [" + " { \"trans_id\":\"t1\", \"amount\":100, " +
        "\"trans_time\":7777777, \"type\":\"sports\"}," +
        " { \"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"}");
      batchString.append("]},");
    }
    batchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," +
      "\"c\" : [" + " { \"trans_id\":\"t1\", \"amount\":100, \"trans_time\":7777777," +
      " \"type\":\"sports\"}," +
      " { \"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"}");
    batchString.append("]}]");
    inputJsonBatches.add(batchString.toString());

    // Figure out what will be approximate total output size out of flatten for input above
    // We will use this sizing information to set output batch size so we can produce desired
    // number of batches that can be verified.

    // output rows will be like this.
    // "a" : 5, "b" : wideString, "c" : {"trans_id":"t1", amount:100, trans_time:7777777, type:sports}
    // "a" : 5, "b" : wideString, "c" : {"trans_id":"t1", amount:100, trans_time:8888888, type:groceries}
    List<String> expectedJsonBatches = Lists.newArrayList();
    StringBuilder expectedBatchString = new StringBuilder();

    expectedBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : " +
        "{\"trans_id\":\"t1\", \"amount\":100, " +
        "\"trans_time\":7777777, \"type\":\"sports\"}},");
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : " +
        "{\"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"}},");
    }

    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : " +
      "{\"trans_id\":\"t1\", \"amount\":100, " +
      "\"trans_time\":7777777, \"type\":\"sports\"}},");
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : " +
      "{\"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"}}");

    expectedBatchString.append("]");
    expectedJsonBatches.add(expectedBatchString.toString());

    long totalSize = getExpectedSize(expectedJsonBatches);
    // set the output batch size to 1/2 of total size expected.
    // We will get approximately 4 batches because of fragmentation factor of 2 accounted for in flatten.
    fragContext.getOptions().setLocalOption("drill.exec.memory.operator.output_batch_size", totalSize / 2);

    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(flatten)
      .inputDataStreamJson(inputJsonBatches)
      .baselineColumns("a", "b", "c")
      .expectedNumBatches(4) // verify number of batches
      .expectedBatchSize(totalSize / 2);  // verify batch size.

    JsonStringHashMap<String, Object> resultExpected1 = new JsonStringHashMap<>();
    resultExpected1.put("trans_id", new Text("t1"));
    resultExpected1.put("amount", new Long(100));
    resultExpected1.put("trans_time", new Long(7777777));
    resultExpected1.put("type", new Text("sports"));

    JsonStringHashMap<String, Object> resultExpected2 = new JsonStringHashMap<>();
    resultExpected2.put("trans_id", new Text("t2"));
    resultExpected2.put("amount", new Long(1000));
    resultExpected2.put("trans_time", new Long(8888888));
    resultExpected2.put("type", new Text("groceries"));

    for (int i = 0; i < numRows + 1; i++) {
      opTestBuilder.baselineValues(5l, wideString, resultExpected1);
      opTestBuilder.baselineValues(5l, wideString, resultExpected2);
    }

    opTestBuilder.go();
  }

  @Test
  public void testFlattenListOfMaps() throws Exception {
    PhysicalOperator flatten = new FlattenPOP(null, SchemaPath.getSimplePath("c"));
    mockOpContext(flatten, initReservation, maxAllocation);

    // create input rows like this.
    // "a" : 5, "b" : wideString,
    // "c" : [ [{"trans_id":"t1", amount:100, trans_time:7777777, type:sports}, {"trans_id":"t1", amount:100, trans_time:8888888, type:groceries}],
    //         [{"trans_id":"t1", amount:100, trans_time:7777777, type:sports}, {"trans_id":"t1", amount:100, trans_time:8888888, type:groceries}],
    //         [{"trans_id":"t1", amount:100, trans_time:7777777, type:sports}, {"trans_id":"t1", amount:100, trans_time:8888888, type:groceries}] ]

    List<String> inputJsonBatches = Lists.newArrayList();
    StringBuilder batchString = new StringBuilder();

    batchString.append("[");
    for (int i = 0; i < numRows; i++) {
      batchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : [" +
        "[ { \"trans_id\":\"t1\", \"amount\":100, \"trans_time\":7777777, \"type\":\"sports\"}," + " { " +
        "\"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"} ], " +
        "[ { \"trans_id\":\"t1\", \"amount\":100, \"trans_time\":7777777, \"type\":\"sports\"}," + " { " +
        "\"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"} ], " +
        "[ { \"trans_id\":\"t1\", \"amount\":100, " +
        "\"trans_time\":7777777, \"type\":\"sports\"}," + " { " +
        "\"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"} ]");
      batchString.append("]},");
    }
    batchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : [" +
      "[ { \"trans_id\":\"t1\", \"amount\":100, \"trans_time\":7777777, \"type\":\"sports\"}," + " { " +
      "\"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"} ], " +
      "[ { \"trans_id\":\"t1\", \"amount\":100, \"trans_time\":7777777, \"type\":\"sports\"}," + " { " +
      "\"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"} ], " +
      "[ { \"trans_id\":\"t1\", \"amount\":100, " +
      "\"trans_time\":7777777, \"type\":\"sports\"}," + " { " +
      "\"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"} ]");
    batchString.append("]}]");
    inputJsonBatches.add(batchString.toString());

    // Figure out what will be approximate total output size out of flatten for input above
    // We will use this sizing information to set output batch size so we can produce desired
    // number of batches that can be verified.

    // output rows will be like this.
    // "a" : 5, "b" : wideString, "c" : [{"trans_id":"t1", amount:100, trans_time:7777777, type:sports}, {"trans_id":"t1", amount:100, trans_time:8888888, type:groceries}]
    // "a" : 5, "b" : wideString, "c" : [{"trans_id":"t1", amount:100, trans_time:7777777, type:sports}, {"trans_id":"t1", amount:100, trans_time:8888888, type:groceries}]
    List<String> expectedJsonBatches = Lists.newArrayList();
    StringBuilder expectedBatchString = new StringBuilder();

    expectedBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : " +
        "[ { \"trans_id\":\"t1\", \"amount\":100, \"trans_time\":7777777, \"type\":\"sports\"}," + " { " +
        "\"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"} ]},");
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : " +
        "[ { \"trans_id\":\"t1\", \"amount\":100, " +
        "\"trans_time\":7777777, \"type\":\"sports\"}," + " { " +
        "\"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"}]},");
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : " +
        "[ { \"trans_id\":\"t1\", \"amount\":100, " +
        "\"trans_time\":7777777, \"type\":\"sports\"}," + " { " +
        "\"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"}]},");
    }

    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : " +
      "[ { \"trans_id\":\"t1\", \"amount\":100, \"trans_time\":7777777, \"type\":\"sports\"}," + " { " +
      "\"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"} ]},");
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : " +
      "[ { \"trans_id\":\"t1\", \"amount\":100, " +
      "\"trans_time\":7777777, \"type\":\"sports\"}," + " { " +
      "\"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"}]},");
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : " +
      "[ { \"trans_id\":\"t1\", \"amount\":100, " +
      "\"trans_time\":7777777, \"type\":\"sports\"}," + " { " +
      "\"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"}]}");

    expectedBatchString.append("]");
    expectedJsonBatches.add(expectedBatchString.toString());

    long totalSize = getExpectedSize(expectedJsonBatches);
    // set the output batch size to 1/2 of total size expected.
    // We will get approximately 4 batches because of fragmentation factor of 2 accounted for in flatten.
    fragContext.getOptions().setLocalOption("drill.exec.memory.operator.output_batch_size", totalSize / 2);

    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(flatten)
      .inputDataStreamJson(inputJsonBatches)
      .baselineColumns("a", "b", "c")
      .expectedNumBatches(4) // verify number of batches
      .expectedBatchSize(totalSize / 2);  // verify batch size.

    final JsonStringHashMap<String, Object> resultExpected1 = new JsonStringHashMap<>();
    resultExpected1.put("trans_id", new Text("t1"));
    resultExpected1.put("amount", new Long(100));
    resultExpected1.put("trans_time", new Long(7777777));
    resultExpected1.put("type", new Text("sports"));

    final JsonStringHashMap<String, Object> resultExpected2 = new JsonStringHashMap<>();
    resultExpected2.put("trans_id", new Text("t2"));
    resultExpected2.put("amount", new Long(1000));
    resultExpected2.put("trans_time", new Long(8888888));
    resultExpected2.put("type", new Text("groceries"));

    final JsonStringArrayList<JsonStringHashMap<String, Object>> results = new JsonStringArrayList<JsonStringHashMap<String, Object>>() {{
      add(resultExpected1);
      add(resultExpected2);
    }};

    for (int i = 0; i < numRows + 1; i++) {
      opTestBuilder.baselineValues(5l, wideString, results);
      opTestBuilder.baselineValues(5l, wideString, results);
      opTestBuilder.baselineValues(5l, wideString, results);
    }

    opTestBuilder.go();
  }

  @Test
  public void testFlattenNestedMap() throws Exception {
    PhysicalOperator flatten = new FlattenPOP(null, SchemaPath.getSimplePath("c"));
    mockOpContext(flatten, initReservation, maxAllocation);

    // create input rows like this.
    // "a" : 5, "b" : wideString,
    // "c" : [ {innerMap: {"trans_id":"t1", amount:100, trans_time:7777777, type:sports}, "trans_id":"t1", amount:100, trans_time:8888888, type:groceries},
    //         {innerMap: {"trans_id":"t1", amount:100, trans_time:7777777, type:sports}, "trans_id":"t1", amount:100, trans_time:8888888, type:groceries} ]

    List<String> inputJsonBatches = Lists.newArrayList();
    StringBuilder batchString = new StringBuilder();

    StringBuilder innerMap = new StringBuilder();
    innerMap.append("{ \"trans_id\":\"inner_trans_t1\", \"amount\":100, \"trans_time\":7777777, \"type\":\"sports\"}");

    batchString.append("[");
    for (int i = 0; i < numRows; i++) {
      batchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : [" +
        " { \"innerMap\": " + innerMap + ", \"trans_id\":\"t1\", \"amount\":100, \"trans_time\":7777777, " +
        "\"type\":\"sports\"}," + " { \"innerMap\": " + innerMap +
        ", \"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"}]");
      batchString.append("},");
    }
    batchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : [" +
      " { \"innerMap\": " + innerMap + ", \"trans_id\":\"t1\", \"amount\":100, \"trans_time\":7777777, " +
      "\"type\":\"sports\"}," + " { \"innerMap\": " + innerMap + ",  \"trans_id\":\"t2\", \"amount\":1000, \"trans_time\":8888888, \"type\":\"groceries\"}");
    batchString.append("]}]");
    inputJsonBatches.add(batchString.toString());

    // Figure out what will be approximate total output size out of flatten for input above
    // We will use this sizing information to set output batch size so we can produce desired
    // number of batches that can be verified.

    // output rows will be like this.
    // "a" : 5, "b" : wideString, "c" : {innerMap: {"trans_id":"t1", amount:100, trans_time:7777777, type:sports}, "trans_id":"t1", amount:100, trans_time:8888888, type:groceries}
    // "a" : 5, "b" : wideString, "c" : {innerMap: {"trans_id":"t1", amount:100, trans_time:7777777, type:sports}, "trans_id":"t1", amount:100, trans_time:8888888, type:groceries}
    List<String> expectedJsonBatches = Lists.newArrayList();
    StringBuilder expectedBatchString = new StringBuilder();

    expectedBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : " +
        " { \"innerMap\": " + innerMap + ", \"trans_id\":\"t1\", \"amount\":100, \"trans_time\":7777777, " +
        "\"type\":\"sports\"} }, ");
      expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : " +
        " { \"innerMap\": " + innerMap + ", \"trans_id\":\"t1\", \"amount\":100, \"trans_time\":7777777, " +
        "\"type\":\"sports\"} }, ");
    }

    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : " +
      " { \"innerMap\": " + innerMap + ", \"trans_id\":\"t1\", \"amount\":100, \"trans_time\":7777777, " +
      "\"type\":\"sports\"} }, ");
    expectedBatchString.append("{\"a\": 5, " + "\"b\" : " + "\"" + wideString + "\"," + "\"c\" : " +
      " { \"innerMap\": " + innerMap + ", \"trans_id\":\"t1\", \"amount\":100, \"trans_time\":7777777, " +
      "\"type\":\"sports\"} }");

    expectedBatchString.append("]");
    expectedJsonBatches.add(expectedBatchString.toString());

    long totalSize = getExpectedSize(expectedJsonBatches);
    // set the output batch size to 1/2 of total size expected.
    // We will get approximately 4 batches because of fragmentation factor of 2 accounted for in flatten.
    fragContext.getOptions().setLocalOption("drill.exec.memory.operator.output_batch_size", totalSize / 2);

    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(flatten)
      .inputDataStreamJson(inputJsonBatches)
      .baselineColumns("a", "b", "c")
      .expectedNumBatches(4) // verify number of batches
      .expectedBatchSize(totalSize / 2);  // verify batch size.

    JsonStringHashMap<String, Object> innerMapResult = new JsonStringHashMap<>();
    innerMapResult.put("trans_id", new Text("inner_trans_t1"));
    innerMapResult.put("amount", new Long(100));
    innerMapResult.put("trans_time", new Long(7777777));
    innerMapResult.put("type", new Text("sports"));

    JsonStringHashMap<String, Object> resultExpected1 = new JsonStringHashMap<>();
    resultExpected1.put("trans_id", new Text("t1"));
    resultExpected1.put("amount", new Long(100));
    resultExpected1.put("trans_time", new Long(7777777));
    resultExpected1.put("type", new Text("sports"));
    resultExpected1.put("innerMap", innerMapResult);

    JsonStringHashMap<String, Object> resultExpected2 = new JsonStringHashMap<>();
    resultExpected2.put("trans_id", new Text("t2"));
    resultExpected2.put("amount", new Long(1000));
    resultExpected2.put("trans_time", new Long(8888888));
    resultExpected2.put("type", new Text("groceries"));
    resultExpected2.put("innerMap", innerMapResult);

    for (int i = 0; i < numRows + 1; i++) {
      opTestBuilder.baselineValues(5l, wideString, resultExpected1);
      opTestBuilder.baselineValues(5l, wideString, resultExpected2);
    }

    opTestBuilder.go();
  }

  @Test
  public void testFlattenUpperLimit() throws Exception {
    // test the upper limit of 65535 records per batch.
    PhysicalOperator flatten = new FlattenPOP(null, SchemaPath.getSimplePath("c"));
    mockOpContext(flatten, initReservation, maxAllocation);
    List<String> inputJsonBatches = Lists.newArrayList();
    StringBuilder batchString = new StringBuilder();

    StringBuilder flattenElement = new StringBuilder();

    // Create list of 1000 elements
    flattenElement.append("[");
    for (int i = 0; i < 1000; i++) {
      flattenElement.append(i);
      flattenElement.append(",");
    }
    flattenElement.append(1000);
    flattenElement.append("]");

    batchString.append("[");

    numRows = 100;

    for (int i = 0; i < numRows; i++) {
      batchString.append("{\"a\": 5, "  + "\"c\":" + flattenElement + "},");
    }
    batchString.append("{\"a\": 5, " + "\"c\":" + flattenElement + "}");
    batchString.append("]");
    inputJsonBatches.add(batchString.toString());

    // Figure out what will be approximate total output size out of flatten for input above
    // We will use this sizing information to set output batch size so we can produce desired
    // number of batches that can be verified.
    List<String> expectedJsonBatches = Lists.newArrayList();
    StringBuilder expectedBatchString = new StringBuilder();

    expectedBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      for (int j = 0; j < 1000; j++) {
        expectedBatchString.append("{\"a\": 5, "  + "\"c\" :");
        expectedBatchString.append(j);
        expectedBatchString.append("},");
      }
    }
    for (int j = 0; j < 999; j++) {
      expectedBatchString.append("{\"a\": 5, "  + "\"c\" :");
      expectedBatchString.append(j);
      expectedBatchString.append("},");
    }

    expectedBatchString.append("{\"a\": 5, "  + "\"c\" :");
    expectedBatchString.append(1000);
    expectedBatchString.append("}");

    expectedBatchString.append("]");
    expectedJsonBatches.add(expectedBatchString.toString());

    long totalSize = getExpectedSize(expectedJsonBatches);
    // set the output batch size to 1/2 of total size expected.
    // We will get approximately 4 batches because of fragmentation factor of 2 accounted for in flatten.
    fragContext.getOptions().setLocalOption("drill.exec.memory.operator.output_batch_size", totalSize / 2);

    // Here we expect 16 batches because each batch will be limited by upper limit of 65535 records.
    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(flatten)
      .inputDataStreamJson(inputJsonBatches)
      .baselineColumns("a", "c")
      .expectedNumBatches(2) // verify number of batches
      .expectedBatchSize(totalSize / 2);  // verify batch size.

    for (long i = 0; i < numRows + 1; i++) {
      for (long j = 0; j < 1001; j++) {
        opTestBuilder.baselineValues(5l, j);
      }
    }

    opTestBuilder.go();
  }

  @Test
  public void testFlattenLowerLimit() throws Exception {
    // test the lower limit of at least one batch
    PhysicalOperator flatten = new FlattenPOP(null, SchemaPath.getSimplePath("c"));
    mockOpContext(flatten, initReservation, maxAllocation);

    List<String> inputJsonBatches = Lists.newArrayList();
    StringBuilder batchString = new StringBuilder();
    StringBuilder flattenElement = new StringBuilder();

    // Create list of 10 elements
    flattenElement.append("[");
    for (int i = 0; i < 10; i++) {
      flattenElement.append(i);
      flattenElement.append(",");
    }
    flattenElement.append(10);
    flattenElement.append("]");

    // create list of wideStrings
    final StringBuilder wideStrings = new StringBuilder();
    wideStrings.append("[");
    for (int i = 0; i < 10; i++) {
      wideStrings.append("\"" + wideString + "\",");
    }
    wideStrings.append("\"" + wideString + "\"");
    wideStrings.append("]");

    batchString.append("[");
    batchString.append("{\"a\": " + wideStrings + "," + "\"c\":" + flattenElement);
    batchString.append("}]");
    inputJsonBatches.add(batchString.toString());

    // Figure out what will be approximate total output size out of flatten for input above
    // We will use this sizing information to set output batch size so we can produce desired
    // number of batches that can be verified.

    // set very low value of batch size for a large record size.
    // This is to test we atleast get one record per batch.
    fragContext.getOptions().setLocalOption("drill.exec.memory.operator.output_batch_size", 1024);

    // Here we expect 10 batches because each batch will be bounded by lower limit of at least 1 record.
    // do not check the output batch size as it will be more than configured value of 1024, so we get
    // at least one record out.
    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(flatten)
      .inputDataStreamJson(inputJsonBatches)
      .baselineColumns("a", "c")
      .expectedNumBatches(10); // verify number of batches

    final JsonStringArrayList<Text> results = new JsonStringArrayList<Text>() {{
      add(new Text(wideString));
      add(new Text(wideString));
      add(new Text(wideString));
      add(new Text(wideString));
      add(new Text(wideString));
      add(new Text(wideString));
      add(new Text(wideString));
      add(new Text(wideString));
      add(new Text(wideString));
      add(new Text(wideString));
      add(new Text(wideString));
    }};

    for (long j = 0; j < 11; j++) {
      opTestBuilder.baselineValues(results, j);
    }

    opTestBuilder.go();
  }

  @Test
  public void testFlattenEmptyList() throws Exception {
    final PhysicalOperator flatten = new FlattenPOP(null, SchemaPath.getSimplePath("b"));

    List<String> inputJsonBatches = Lists.newArrayList();
    StringBuilder batchString = new StringBuilder();

    StringBuilder flattenElement = new StringBuilder();

    flattenElement.append("[");
    flattenElement.append("]");

    batchString.append("[");
    for (int i = 0; i < numRows; i++) {
      batchString.append("{\"a\": 5, " + "\"b\" : " + flattenElement + "},");
    }
    batchString.append("{\"a\": 5, " + "\"b\" : " + flattenElement + "}");
    batchString.append("]");
    inputJsonBatches.add(batchString.toString());

    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(flatten)
      .inputDataStreamJson(inputJsonBatches)
      .baselineColumns("a", "b")
      .expectZeroRows();

    opTestBuilder.go();
  }

  @Test
  public void testFlattenLargeRecords() throws Exception {
    PhysicalOperator flatten = new FlattenPOP(null, SchemaPath.getSimplePath("c"));
    mockOpContext(flatten, initReservation, maxAllocation);

    // create input rows like this.
    // "a" : <id1>, "b" : wideString, "c" : [ 10 wideStrings ]
    List<String> inputJsonBatches = Lists.newArrayList();
    StringBuilder batchString = new StringBuilder();
    int arrayLength = 10;
    StringBuilder test = new StringBuilder();
    test.append("[ \"");
    for (int i = 0; i < arrayLength; i++) {
      test.append(wideString);
      test.append("\",\"");
    }
    test.append(wideString);
    test.append("\"]");

    batchString.append("[");
    for (int i = 0; i < numRows; i++) {
      batchString.append("{" + "\"a\" :" + (new StringBuilder().append(i)) + ",\"b\": \"" + wideString + "\"," +
        "\"c\": " + test + "},");
    }
    batchString.append("{" + "\"a\" :" + (new StringBuilder().append(numRows)) + ",\"b\": \"" + wideString + "\"," +
      "\"c\": " + test + "}");
    batchString.append("]");
    inputJsonBatches.add(batchString.toString());

    // output rows will be like this.
    // "a" : <id1>, "b" : wideString, "c" : wideString

    // Figure out what will be approximate total output size out of flatten for input above
    // We will use this sizing information to set output batch size so we can produce desired
    // number of batches that can be verified.
    List<String> expectedJsonBatches = Lists.newArrayList();
    StringBuilder expectedBatchString = new StringBuilder();
    expectedBatchString.append("[");
    for (int k = 0; k < (numRows) * 11; k++) {
      expectedBatchString.append("{" + "\"a\" :" + (new StringBuilder().append(k)) + ",\"b\": \"" + wideString + "\",");
      expectedBatchString.append("\"c\": \"" + wideString + "\"},");
    }
    expectedBatchString.append("{" + "\"a\" :" + (new StringBuilder().append(numRows)) + ",\"b\": \"" + wideString + "\",");
    expectedBatchString.append("\"c\": \"" + wideString + "\"}");
    expectedBatchString.append("]");
    expectedJsonBatches.add(expectedBatchString.toString());

    long totalSize = getExpectedSize(expectedJsonBatches);
    // set the output batch size to 1/2 of total size expected.
    // We will get approximately 4 batches because of fragmentation factor of 2 accounted for in flatten.
    fragContext.getOptions().setLocalOption("drill.exec.memory.operator.output_batch_size", totalSize / 2);

    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(flatten)
      .inputDataStreamJson(inputJsonBatches)
      .baselineColumns("a", "b", "c")
      .expectedNumBatches(4) // verify number of batches
      .expectedBatchSize(totalSize / 2);  // verify batch size.

    for (long k = 0; k < ((numRows + 1)); k++) {
      for (int j = 0; j < arrayLength + 1; j++) {
        opTestBuilder.baselineValues(k, wideString, wideString);
      }
    }

    opTestBuilder.go();
  }

  @Test
  public void testMergeJoinMultipleOutputBatches() throws Exception {
    MergeJoinPOP mergeJoin = new MergeJoinPOP(null, null,
      Lists.newArrayList(joinCond("c1", "EQUALS", "c2")), JoinRelType.INNER);
    mockOpContext(mergeJoin, initReservation, maxAllocation);

    // create left input rows like this.
    // "a1" : 5, "b1" : wideString, "c1" : <id>
    List<String> leftJsonBatches = Lists.newArrayList();
    StringBuilder leftBatchString = new StringBuilder();
    leftBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      leftBatchString.append("{\"a1\": 5, " + "\"b1\" : " + "\"" + wideString + "\"," + "\"c1\" : " + i + "},");
    }
    leftBatchString.append("{\"a1\": 5, " + "\"b1\" : " + "\"" + wideString + "\"," + "\"c1\" : " + numRows + "}");
    leftBatchString.append("]");

    leftJsonBatches.add(leftBatchString.toString());

    // create right input rows like this.
    // "a2" : 6, "b2" : wideString, "c2" : <id>
    List<String> rightJsonBatches = Lists.newArrayList();
    StringBuilder rightBatchString = new StringBuilder();
    rightBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      rightBatchString.append("{\"a2\": 6, " + "\"b2\" : " + "\"" + wideString + "\"," + "\"c2\" : " + i + "},");
    }
    rightBatchString.append("{\"a2\": 6, " + "\"b2\" : " + "\"" + wideString + "\"," + "\"c2\" : " + numRows + "}");
    rightBatchString.append("]");
    rightJsonBatches.add(rightBatchString.toString());

    // output rows will be like this.
    // "a1" : 5, "b1" : wideString, "c1" : 1, "a2":6, "b2" : wideString, "c2": 1
    // "a1" : 5, "b1" : wideString, "c1" : 2, "a2":6, "b2" : wideString, "c2": 2
    // "a1" : 5, "b1" : wideString, "c1" : 3, "a2":6, "b2" : wideString, "c2": 3
    List<String> expectedJsonBatches = Lists.newArrayList();
    StringBuilder expectedBatchString = new StringBuilder();
    expectedBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      expectedBatchString.append("{\"a1\": 5, " + "\"b1\" : " + "\"" + wideString + "\"," + "\"c1\" : " + i);
      expectedBatchString.append(", \"a2\": 6, " + "\"b2\" : " + "\"" + wideString + "\"," + "\"c2\" : " + i + "},");
    }
    expectedBatchString.append("{\"a1\": 5, " + "\"b1\" : " + "\"" + wideString + "\"," + "\"c1\" : " + numRows);
    expectedBatchString.append(", \"a2\": 6, " + "\"b2\" : " + "\"" + wideString + "\"," + "\"c2\" : " + numRows + "}");
    expectedBatchString.append("]");
    expectedJsonBatches.add(expectedBatchString.toString());

    long totalSize = getExpectedSize(expectedJsonBatches);

    // set the output batch size to 1/2 of total size expected.
    // We will get approximately 4 batches because of fragmentation factor of 2 accounted for in merge join.
    fragContext.getOptions().setLocalOption("drill.exec.memory.operator.output_batch_size", totalSize/2);

    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(mergeJoin)
      .baselineColumns("a1", "b1", "c1", "a2", "b2", "c2")
      .expectedNumBatches(4)  // verify number of batches
      .expectedBatchSize(totalSize / 2) // verify batch size
      .inputDataStreamsJson(Lists.newArrayList(leftJsonBatches, rightJsonBatches));

    for (long i = 0; i < numRows + 1; i++) {
      opTestBuilder.baselineValues(5l, wideString, i, 6l, wideString, i);
    }

    opTestBuilder.go();
  }

  @Test
  public void testMergeJoinSingleOutputBatch() throws Exception {
    MergeJoinPOP mergeJoin = new MergeJoinPOP(null, null,
      Lists.newArrayList(joinCond("c1", "EQUALS", "c2")), JoinRelType.INNER);
    mockOpContext(mergeJoin, initReservation, maxAllocation);

    // create multiple batches from both sides.
    numRows = 4096 * 2;

    // create left input rows like this.
    // "a1" : 5, "b1" : wideString, "c1" : <id>
    List<String> leftJsonBatches = Lists.newArrayList();
    StringBuilder leftBatchString = new StringBuilder();
    leftBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      leftBatchString.append("{\"a1\": 5, " + "\"b1\" : " + "\"" + wideString + "\"," + "\"c1\" : " + i + "},");
    }
    leftBatchString.append("{\"a1\": 5, " + "\"b1\" : " + "\"" + wideString + "\"," + "\"c1\" : " + numRows + "}");
    leftBatchString.append("]");

    leftJsonBatches.add(leftBatchString.toString());

    // create right input rows like this.
    // "a2" : 6, "b2" : wideString, "c2" : <id>
    List<String> rightJsonBatches = Lists.newArrayList();
    StringBuilder rightBatchString = new StringBuilder();
    rightBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      rightBatchString.append("{\"a2\": 6, " + "\"b2\" : " + "\"" + wideString + "\"," + "\"c2\" : " + i + "},");
    }
    rightBatchString.append("{\"a2\": 6, " + "\"b2\" : " + "\"" + wideString + "\"," + "\"c2\" : " + numRows + "}");
    rightBatchString.append("]");
    rightJsonBatches.add(rightBatchString.toString());

    // output rows will be like this.
    // "a1" : 5, "b1" : wideString, "c1" : 1, "a2":6, "b2" : wideString, "c2": 1
    // "a1" : 5, "b1" : wideString, "c1" : 2, "a2":6, "b2" : wideString, "c2": 2
    // "a1" : 5, "b1" : wideString, "c1" : 3, "a2":6, "b2" : wideString, "c2": 3
    List<String> expectedJsonBatches = Lists.newArrayList();
    StringBuilder expectedBatchString = new StringBuilder();
    expectedBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      expectedBatchString.append("{\"a1\": 5, " + "\"b1\" : " + "\"" + wideString + "\"," + "\"c1\" : " + i);
      expectedBatchString.append(", \"a2\": 6, " + "\"b2\" : " + "\"" + wideString + "\"," + "\"c2\" : " + i + "},");
    }
    expectedBatchString.append("{\"a1\": 5, " + "\"b1\" : " + "\"" + wideString + "\"," + "\"c1\" : " + numRows);
    expectedBatchString.append(", \"a2\": 6, " + "\"b2\" : " + "\"" + wideString + "\"," + "\"c2\" : " + numRows + "}");
    expectedBatchString.append("]");
    expectedJsonBatches.add(expectedBatchString.toString());

    long totalSize = getExpectedSize(expectedJsonBatches);

    // set the output batch size to twice of total size expected.
    // We should get 1 batch.
    fragContext.getOptions().setLocalOption("drill.exec.memory.operator.output_batch_size", totalSize*2);

    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(mergeJoin)
      .baselineColumns("a1", "b1", "c1", "a2", "b2", "c2")
      .expectedNumBatches(1)  // verify number of batches
      .expectedBatchSize(totalSize) // verify batch size
      .inputDataStreamsJson(Lists.newArrayList(leftJsonBatches, rightJsonBatches));

    for (long i = 0; i < numRows + 1; i++) {
      opTestBuilder.baselineValues(5l, wideString, i, 6l, wideString, i);
    }

    opTestBuilder.go();
  }

  @Test
  public void testMergeJoinUpperLimit() throws Exception {
    // test the upper limit of 65535 records per batch.
    MergeJoinPOP mergeJoin = new MergeJoinPOP(null, null,
      Lists.newArrayList(joinCond("c1", "EQUALS", "c2")), JoinRelType.LEFT);
    mockOpContext(mergeJoin, initReservation, maxAllocation);

    numRows = 100000;

    // create left input rows like this.
    // "a1" : 5,  "c1" : <id>
    List<String> leftJsonBatches = Lists.newArrayList();
    StringBuilder leftBatchString = new StringBuilder();
    leftBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      leftBatchString.append("{\"a1\": 5, " + "\"c1\" : " + i + "},");
    }
    leftBatchString.append("{\"a1\": 5, " + "\"c1\" : " + numRows + "}");
    leftBatchString.append("]");

    leftJsonBatches.add(leftBatchString.toString());

    // create right input rows like this.
    // "a2" : 6, "c2" : <id>
    List<String> rightJsonBatches = Lists.newArrayList();
    StringBuilder rightBatchString = new StringBuilder();
    rightBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      rightBatchString.append("{\"a2\": 6, " + "\"c2\" : " + i + "},");
    }
    rightBatchString.append("{\"a2\": 6, " + "\"c2\" : " + numRows + "}");
    rightBatchString.append("]");
    rightJsonBatches.add(rightBatchString.toString());

    // output rows will be like this.
    // "a1" : 5,  "c1" : 1, "a2":6,  "c2": 1
    // "a1" : 5,  "c1" : 2, "a2":6,  "c2": 2
    // "a1" : 5,  "c1" : 3, "a2":6,  "c2": 3

    // expect two batches, batch limited by 65535 records
    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(mergeJoin)
      .baselineColumns("a1", "c1", "a2", "c2")
      .expectedNumBatches(2)  // verify number of batches
      .inputDataStreamsJson(Lists.newArrayList(leftJsonBatches, rightJsonBatches));

    for (long i = 0; i < numRows + 1; i++) {
      opTestBuilder.baselineValues(5l, i, 6l, i);
    }

    opTestBuilder.go();
  }

  @Test
  public void testMergeJoinLowerLimit() throws Exception {
    // test the lower limit of at least one batch
    MergeJoinPOP mergeJoin = new MergeJoinPOP(null, null,
      Lists.newArrayList(joinCond("c1", "EQUALS", "c2")), JoinRelType.RIGHT);
    mockOpContext(mergeJoin, initReservation, maxAllocation);

    numRows = 10;

    // create left input rows like this.
    // "a1" : 5, "b1" : wideString, "c1" : <id>
    List<String> leftJsonBatches = Lists.newArrayList();
    StringBuilder leftBatchString = new StringBuilder();
    leftBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      leftBatchString.append("{\"a1\": 5, " + "\"b1\" : " + "\"" + wideString + "\"," + "\"c1\" : " + i + "},");
    }
    leftBatchString.append("{\"a1\": 5, " + "\"b1\" : " + "\"" + wideString + "\"," + "\"c1\" : " + numRows + "}");
    leftBatchString.append("]");

    leftJsonBatches.add(leftBatchString.toString());

    // create right input rows like this.
    // "a2" : 6, "b2" : wideString, "c2" : <id>
    List<String> rightJsonBatches = Lists.newArrayList();
    StringBuilder rightBatchString = new StringBuilder();
    rightBatchString.append("[");
    for (int i = 0; i < numRows; i++) {
      rightBatchString.append("{\"a2\": 6, " + "\"b2\" : " + "\"" + wideString + "\"," + "\"c2\" : " + i + "},");
    }
    rightBatchString.append("{\"a2\": 6, " + "\"b2\" : " + "\"" + wideString + "\"," + "\"c2\" : " + numRows + "}");
    rightBatchString.append("]");
    rightJsonBatches.add(rightBatchString.toString());

    // output rows will be like this.
    // "a1" : 5, "b1" : wideString, "c1" : 1, "a2":6, "b2" : wideString, "c2": 1
    // "a1" : 5, "b1" : wideString, "c1" : 2, "a2":6, "b2" : wideString, "c2": 2
    // "a1" : 5, "b1" : wideString, "c1" : 3, "a2":6, "b2" : wideString, "c2": 3

    // set very low value of output batch size so we can do only one row per batch.
    fragContext.getOptions().setLocalOption("drill.exec.memory.operator.output_batch_size", 128);

    OperatorTestBuilder opTestBuilder = opTestBuilder()
      .physicalOperator(mergeJoin)
      .baselineColumns("a1", "b1", "c1", "a2", "b2", "c2")
      .expectedNumBatches(10)  // verify number of batches
      .inputDataStreamsJson(Lists.newArrayList(leftJsonBatches, rightJsonBatches));

    for (long i = 0; i < numRows + 1; i++) {
      opTestBuilder.baselineValues(5l, wideString, i, 6l, wideString, i);
    }

    opTestBuilder.go();
  }
}