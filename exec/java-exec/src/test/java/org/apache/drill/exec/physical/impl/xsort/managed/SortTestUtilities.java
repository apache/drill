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
package org.apache.drill.exec.physical.impl.xsort.managed;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.ops.OperExecContext;
import org.apache.drill.exec.physical.config.Sort;
import org.apache.drill.exec.physical.impl.xsort.managed.PriorityQueueCopierWrapper.BatchMerger;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.test.OperatorFixture;
import org.apache.drill.test.rowSet.DirectRowSet;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetSchema;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;

import com.google.common.collect.Lists;

public class SortTestUtilities {

  private SortTestUtilities() { }

  public static BatchSchema makeSchema(MinorType type, boolean nullable) {
    return new SchemaBuilder()
        .add("key", type, nullable ? DataMode.OPTIONAL : DataMode.REQUIRED)
        .add("value", MinorType.VARCHAR)
        .build();
  }

  public static BatchSchema nonNullSchema() {
    return makeSchema(MinorType.INT, false);
  }

  public static BatchSchema nullableSchema() {
    return makeSchema(MinorType.INT, true);
  }

  public static PriorityQueueCopierWrapper makeCopier(OperatorFixture fixture, String sortOrder, String nullOrder) {
    FieldReference expr = FieldReference.getWithQuotedRef("key");
    Ordering ordering = new Ordering(sortOrder, expr, nullOrder);
    Sort popConfig = new Sort(null, Lists.newArrayList(ordering), false);
    OperExecContext opContext = fixture.newOperExecContext(popConfig);
    return new PriorityQueueCopierWrapper(opContext);
  }

  public static class CopierTester {
    List<SingleRowSet> rowSets = new ArrayList<>();
    List<SingleRowSet> expected = new ArrayList<>();
    String sortOrder = Ordering.ORDER_ASC;
    String nullOrder = Ordering.NULLS_UNSPECIFIED;
    private OperatorFixture fixture;

    public CopierTester(OperatorFixture fixture) {
      this.fixture = fixture;
    }

    public void addInput(SingleRowSet input) {
      rowSets.add(input);
    }

    public void addOutput(SingleRowSet output) {
      expected.add(output);
    }

    public void run() throws Exception {
      PriorityQueueCopierWrapper copier = makeCopier(fixture, sortOrder, nullOrder);
      List<BatchGroup> batches = new ArrayList<>();
      RowSetSchema schema = null;
      for (SingleRowSet rowSet : rowSets) {
        batches.add(new BatchGroup.InputBatch(rowSet.container(), rowSet.getSv2(),
                    fixture.allocator(), rowSet.size()));
        if (schema == null) {
          schema = rowSet.schema();
        }
      }
      int rowCount = outputRowCount();
      VectorContainer dest = new VectorContainer();
      @SuppressWarnings("resource")
      BatchMerger merger = copier.startMerge(schema.toBatchSchema(SelectionVectorMode.NONE),
                                             batches, dest, rowCount);

      verifyResults(merger, dest);
      dest.clear();
      merger.close();
    }

    public int outputRowCount() {
      if (! expected.isEmpty()) {
        return expected.get(0).rowCount();
      }
      return 10;
    }

    protected void verifyResults(BatchMerger merger, VectorContainer dest) {
      for (RowSet expectedSet : expected) {
        assertTrue(merger.next());
        RowSet rowSet = new DirectRowSet(fixture.allocator(), dest);
        new RowSetComparison(expectedSet)
              .verifyAndClearAll(rowSet);
      }
      assertFalse(merger.next());
    }
  }

}
