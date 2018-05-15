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
package org.apache.drill.exec.physical.impl.common;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.JoinCondition;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.config.HashJoinPOP;
import org.apache.drill.exec.physical.impl.aggregate.SpilledRecordbatch;
import org.apache.drill.exec.physical.impl.join.HashJoinMemoryCalculator;
import org.apache.drill.exec.physical.impl.join.HashJoinMemoryCalculatorImpl;
import org.apache.drill.exec.physical.impl.join.JoinUtils;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.planner.logical.DrillJoinRel;
import org.apache.drill.exec.proto.ExecProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBatch;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

public class HashPartitionTest {
  @Rule
  public final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  @Test
  public void noSpillBuildSideTest() throws Exception
  {
    new HashPartitionFixture().run(new HashPartitionTestCase() {
      private RowSet buildRowSet;
      private RowSet probeRowSet;

      @Override
      public RecordBatch createBuildBatch(BatchSchema schema, BufferAllocator allocator) {
        buildRowSet = new RowSetBuilder(allocator, schema)
          .addRow(1, "green")
          .addRow(3, "red")
          .addRow(2, "blue")
          .build();
        return new RowSetBatch(buildRowSet);
      }

      @Override
      public void createResultBuildBatch(BatchSchema schema, BufferAllocator allocator) {
      }

      @Override
      public RecordBatch createProbeBatch(BatchSchema schema, BufferAllocator allocator) {
        probeRowSet = new RowSetBuilder(allocator, schema)
          .addRow(.5, "yellow")
          .addRow(1.5, "blue")
          .addRow(2.5, "black")
          .build();
        return new RowSetBatch(probeRowSet);
      }

      @Override
      public void run(SpillSet spillSet,
                      BatchSchema buildSchema,
                      BatchSchema probeSchema,
                      RecordBatch buildBatch,
                      RecordBatch probeBatch,
                      ChainedHashTable baseHashTable,
                      FragmentContext context,
                      OperatorContext operatorContext) throws Exception {

        final HashPartition hashPartition = new HashPartition(context,
          context.getAllocator(),
          baseHashTable,
          buildBatch,
          probeBatch,
          10,
          spillSet,
          0,
          0);

        final HashJoinMemoryCalculator.BuildSidePartitioning noopCalc = new HashJoinMemoryCalculatorImpl.NoopBuildSidePartitioningImpl();

        hashPartition.appendInnerRow(buildRowSet.container(), 0, 10, noopCalc);
        hashPartition.appendInnerRow(buildRowSet.container(), 1, 11, noopCalc);
        hashPartition.appendInnerRow(buildRowSet.container(), 2, 12, noopCalc);
        hashPartition.completeAnInnerBatch(false, false);
        hashPartition.buildContainersHashTableAndHelper();

        {
          int compositeIndex = hashPartition.probeForKey(0, 16);
          Assert.assertEquals(-1, compositeIndex);
        }

        {
          int compositeIndex = hashPartition.probeForKey(1, 12);
          int startIndex = hashPartition.getStartIndex(compositeIndex);
          int nextIndex = hashPartition.getNextIndex(startIndex);

          Assert.assertEquals(2, startIndex);
          Assert.assertEquals(-1, nextIndex);
        }

        {
          int compositeIndex = hashPartition.probeForKey(2, 15);
          Assert.assertEquals(-1, compositeIndex);
        }

        buildRowSet.clear();
        probeRowSet.clear();
        hashPartition.close();
      }
    });
  }

  @Test
  public void spillSingleIncompleteBatchBuildSideTest() throws Exception
  {
    new HashPartitionFixture().run(new HashPartitionTestCase() {
      private RowSet buildRowSet;
      private RowSet probeRowSet;
      private RowSet actualBuildRowSet;

      @Override
      public RecordBatch createBuildBatch(BatchSchema schema, BufferAllocator allocator) {
        buildRowSet = new RowSetBuilder(allocator, schema)
          .addRow(1, "green")
          .addRow(3, "red")
          .addRow(2, "blue")
          .build();
        return new RowSetBatch(buildRowSet);
      }

      @Override
      public void createResultBuildBatch(BatchSchema schema, BufferAllocator allocator) {
        final BatchSchema newSchema = BatchSchema.newBuilder()
          .addFields(schema)
          .addField(MaterializedField.create(HashPartition.HASH_VALUE_COLUMN_NAME, HashPartition.HVtype))
          .build();
        actualBuildRowSet = new RowSetBuilder(allocator, newSchema)
          .addRow(1, "green", 10)
          .addRow(3, "red", 11)
          .addRow(2, "blue", 12)
          .build();
      }

      @Override
      public RecordBatch createProbeBatch(BatchSchema schema, BufferAllocator allocator) {
        probeRowSet = new RowSetBuilder(allocator, schema)
          .addRow(.5, "yellow")
          .addRow(1.5, "blue")
          .addRow(2.5, "black")
          .build();
        return new RowSetBatch(probeRowSet);
      }

      @Override
      public void run(SpillSet spillSet,
                      BatchSchema buildSchema,
                      BatchSchema probeSchema,
                      RecordBatch buildBatch,
                      RecordBatch probeBatch,
                      ChainedHashTable baseHashTable,
                      FragmentContext context,
                      OperatorContext operatorContext) {

        final HashPartition hashPartition = new HashPartition(context,
          context.getAllocator(),
          baseHashTable,
          buildBatch,
          probeBatch,
          10,
          spillSet,
          0,
          0);

        final HashJoinMemoryCalculator.BuildSidePartitioning noopCalc = new HashJoinMemoryCalculatorImpl.NoopBuildSidePartitioningImpl();

        hashPartition.appendInnerRow(buildRowSet.container(), 0, 10, noopCalc);
        hashPartition.appendInnerRow(buildRowSet.container(), 1, 11, noopCalc);
        hashPartition.appendInnerRow(buildRowSet.container(), 2, 12, noopCalc);
        hashPartition.completeAnInnerBatch(false, false);
        hashPartition.spillThisPartition();
        final String spillFile = hashPartition.getSpillFile();
        final int batchesCount = hashPartition.getPartitionBatchesCount();;
        hashPartition.closeWriter();

        SpilledRecordbatch spilledBuildBatch = new SpilledRecordbatch(spillFile, batchesCount, context, buildSchema, operatorContext, spillSet);
        final RowSet actual = DirectRowSet.fromContainer(spilledBuildBatch.getContainer());

        new RowSetComparison(actualBuildRowSet).verify(actual);

        spilledBuildBatch.close();
        buildRowSet.clear();
        actualBuildRowSet.clear();
        probeRowSet.clear();
        hashPartition.close();
      }
    });
  }

  public class HashPartitionFixture {
    public void run(HashPartitionTestCase testCase) throws Exception {
      try (OperatorFixture operatorFixture = new OperatorFixture.Builder(HashPartitionTest.this.dirTestWatcher).build()) {

        final FragmentContext context = operatorFixture.getFragmentContext();
        final HashJoinPOP pop = new HashJoinPOP(null, null, null, JoinRelType.FULL);
        final OperatorContext operatorContext = operatorFixture.operatorContext(pop);
        final DrillConfig config = context.getConfig();
        final BufferAllocator allocator = operatorFixture.allocator();

        final UserBitShared.QueryId queryId = UserBitShared.QueryId.newBuilder()
          .setPart1(1L)
          .setPart2(2L)
          .build();
        final ExecProtos.FragmentHandle fragmentHandle = ExecProtos.FragmentHandle.newBuilder()
          .setQueryId(queryId)
          .setMinorFragmentId(1)
          .setMajorFragmentId(2)
          .build();

        final SpillSet spillSet = new SpillSet(config, fragmentHandle, pop);

        // Create build batch
        MaterializedField buildColA = MaterializedField.create("buildColA", Types.required(TypeProtos.MinorType.INT));
        MaterializedField buildColB = MaterializedField.create("buildColB", Types.required(TypeProtos.MinorType.VARCHAR));
        List<MaterializedField> buildCols = Lists.newArrayList(buildColA, buildColB);
        final BatchSchema buildSchema = new BatchSchema(BatchSchema.SelectionVectorMode.NONE, buildCols);
        final RecordBatch buildBatch = testCase.createBuildBatch(buildSchema, allocator);
        testCase.createResultBuildBatch(buildSchema, allocator);

        // Create probe batch
        MaterializedField probeColA = MaterializedField.create("probeColA", Types.required(TypeProtos.MinorType.FLOAT4));
        MaterializedField probeColB = MaterializedField.create("probeColB", Types.required(TypeProtos.MinorType.VARCHAR));
        List<MaterializedField> probeCols = Lists.newArrayList(probeColA, probeColB);
        final BatchSchema probeSchema = new BatchSchema(BatchSchema.SelectionVectorMode.NONE, probeCols);
        final RecordBatch probeBatch = testCase.createProbeBatch(probeSchema, allocator);

        final LogicalExpression buildColExpression = SchemaPath.getSimplePath(buildColB.getName());;
        final LogicalExpression probeColExpression = SchemaPath.getSimplePath(probeColB.getName());;

        final JoinCondition condition = new JoinCondition(DrillJoinRel.EQUALITY_CONDITION, probeColExpression, buildColExpression);
        final List<Comparator> comparators = Lists.newArrayList(JoinUtils.checkAndReturnSupportedJoinComparator(condition));

        final List<NamedExpression> buildExpressions = Lists.newArrayList(new NamedExpression(buildColExpression, new FieldReference("build_side_0")));
        final List<NamedExpression> probeExpressions = Lists.newArrayList(new NamedExpression(probeColExpression, new FieldReference("probe_side_0")));

        final int hashTableSize = (int) context.getOptions().getOption(ExecConstants.MIN_HASH_TABLE_SIZE);
        final HashTableConfig htConfig = new HashTableConfig(hashTableSize, HashTable.DEFAULT_LOAD_FACTOR, buildExpressions, probeExpressions, comparators);
        final ChainedHashTable baseHashTable = new ChainedHashTable(htConfig, context, allocator, buildBatch, probeBatch, null);
        baseHashTable.updateIncoming(buildBatch, probeBatch);

        testCase.run(spillSet, buildSchema, probeSchema, buildBatch, probeBatch, baseHashTable, context, operatorContext);
      }
    }
  }

  interface HashPartitionTestCase {
    RecordBatch createBuildBatch(BatchSchema schema, BufferAllocator allocator);
    void createResultBuildBatch(BatchSchema schema, BufferAllocator allocator);
    RecordBatch createProbeBatch(BatchSchema schema, BufferAllocator allocator);

    void run(SpillSet spillSet,
             BatchSchema buildSchema,
             BatchSchema probeSchema,
             RecordBatch buildBatch,
             RecordBatch probeBatch,
             ChainedHashTable baseHashTable,
             FragmentContext context,
             OperatorContext operatorContext) throws Exception;
  }
}
