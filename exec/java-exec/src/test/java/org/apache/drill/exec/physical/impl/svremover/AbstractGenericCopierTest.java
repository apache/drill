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
package org.apache.drill.exec.physical.impl.svremover;

import com.google.common.collect.Lists;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.RootAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBatch;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.junit.Test;

import java.util.List;

public abstract class AbstractGenericCopierTest {
  @Test
  public void testCopyRecords() throws SchemaChangeException {
    try (RootAllocator allocator = new RootAllocator(10_000_000)) {
      final BatchSchema batchSchema = createTestSchema(BatchSchema.SelectionVectorMode.NONE);
      final RowSet srcRowSet = createSrcRowSet(allocator);
      final RowSet destRowSet = new RowSetBuilder(allocator, batchSchema).build();
      final VectorContainer destContainer = destRowSet.container();
      final Copier copier = createCopier();
      final RowSet expectedRowSet = createExpectedRowset(allocator);

      copier.setup(new RowSetBatch(srcRowSet), destContainer);
      copier.copyRecords(0, 3);

      try {
        new RowSetComparison(expectedRowSet).verify(destRowSet);
      } finally {
        srcRowSet.clear();

        if (srcRowSet instanceof RowSet.HyperRowSet) {
          ((RowSet.HyperRowSet)srcRowSet).getSv4().clear();
        }

        destRowSet.clear();
        expectedRowSet.clear();
      }
    }
  }

  @Test
  public void testAppendRecords() throws SchemaChangeException {
    try (RootAllocator allocator = new RootAllocator(10_000_000)) {
      final BatchSchema batchSchema = createTestSchema(BatchSchema.SelectionVectorMode.NONE);
      final RowSet srcRowSet = createSrcRowSet(allocator);
      final RowSet destRowSet = new RowSetBuilder(allocator, batchSchema).build();
      final VectorContainer destContainer = destRowSet.container();
      final Copier copier = createCopier();
      final RowSet expectedRowSet = createExpectedRowset(allocator);

      copier.setup(new RowSetBatch(srcRowSet), destContainer);
      copier.appendRecord(0);
      copier.appendRecords(1, 2);

      try {
        new RowSetComparison(expectedRowSet).verify(destRowSet);
      } finally {
        srcRowSet.clear();

        if (srcRowSet instanceof RowSet.HyperRowSet) {
          ((RowSet.HyperRowSet)srcRowSet).getSv4().clear();
        }

        destRowSet.clear();
        expectedRowSet.clear();
      }
    }
  }

  public abstract RowSet createSrcRowSet(RootAllocator allocator) throws SchemaChangeException;

  public abstract Copier createCopier();

  public static Object[] row1() {
    return new Object[]{110, "green", new float[]{5.5f, 2.3f}, new String[]{"1a", "1b"}};
  }

  public static Object[] row2() {
    return new Object[]{109, "blue", new float[]{1.5f}, new String[]{"2a"}};
  }

  public static Object[] row3() {
    return new Object[]{108, "red", new float[]{-11.1f, 0.0f, .5f}, new String[]{"3a", "3b", "3c"}};
  }

  public static Object[] row4() {
    return new Object[]{107, "yellow", new float[]{4.25f, 1.25f}, new String[]{}};
  }

  public static Object[] row5() {
    return new Object[]{106, "black", new float[]{.75f}, new String[]{"4a"}};
  }

  public static RowSet createExpectedRowset(RootAllocator allocator) {
    return new RowSetBuilder(allocator, createTestSchema(BatchSchema.SelectionVectorMode.NONE))
      .addRow(row1())
      .addRow(row2())
      .addRow(row3())
      .build();
  }

  public static BatchSchema createTestSchema(BatchSchema.SelectionVectorMode mode) {
    MaterializedField colA = MaterializedField.create("colA", Types.required(TypeProtos.MinorType.INT));
    MaterializedField colB = MaterializedField.create("colB", Types.required(TypeProtos.MinorType.VARCHAR));
    MaterializedField colC = MaterializedField.create("colC", Types.repeated(TypeProtos.MinorType.FLOAT4));
    MaterializedField colD = MaterializedField.create("colD", Types.repeated(TypeProtos.MinorType.VARCHAR));

    List<MaterializedField> cols = Lists.newArrayList(colA, colB, colC, colD);
    BatchSchema batchSchema = new BatchSchema(mode, cols);
    return batchSchema;
  }
}
