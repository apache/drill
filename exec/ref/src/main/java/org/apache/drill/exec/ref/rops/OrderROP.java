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
package org.apache.drill.exec.ref.rops;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.logical.data.Order;
import org.apache.drill.common.logical.data.Order.Direction;
import org.apache.drill.common.logical.data.Order.NullCollation;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorFactory;
import org.apache.drill.exec.ref.eval.EvaluatorTypes.BasicEvaluator;
import org.apache.drill.exec.ref.values.ComparableValue;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;

public class OrderROP extends AbstractBlockingOperator<Order> {

  private List<CompoundValue> records = new ArrayList<CompoundValue>();
  private SortDefinition[] defs;
  private int withinExtra = 0;
  private boolean withinConstrained;
  private BasicEvaluator within;
  private DataValue previous;
  private long withinMarkerValue;

  public OrderROP(Order config) {
    super(config);
  }

  @Override
  protected void setupEvals(EvaluatorFactory builder) {
    Ordering[] orderings = config.getOrderings();
    withinConstrained = config.getWithin() != null;
    if (withinConstrained) {
      withinExtra = 1;
      within = builder.getBasicEvaluator(record, config.getWithin());
    }

    defs = new SortDefinition[orderings.length];

    for (int i = 0; i < orderings.length; i++) {
      defs[i] = new SortDefinition(builder.getBasicEvaluator(inputRecord, orderings[i].getExpr()),
          orderings[i].getDirection() == Direction.ASC, orderings[i].getNullCollation() == NullCollation.NULLS_LAST);
    }
  }

  @Override
  protected void consumeRecord() {
    DataValue[] values = new DataValue[defs.length + withinExtra];

    RecordPointer r = inputRecord.copy();

    /**
     * Rather than use a sort on the within value, we need to make sure that this operator correctly implements the
     * reference implementation. Ordering only operates with sequential segment key values. If a segment key value
     * repeats elsewhere in the stream, this operator should bring those together. together.
     **/
    if (withinConstrained) {
      DataValue current = within.eval();
      if (!current.equals(previous)) {
        withinMarkerValue++;
      }
      values[0] = new ScalarValues.LongScalar(withinMarkerValue);
    }

    for (int i = 0; i < defs.length; i++) {
      values[i + withinExtra] = defs[i].evaluator.eval();
    }
    CompoundValue v = new CompoundValue(r, values);
    records.add(v);
  }

  @Override
  protected RecordIterator doWork() {
    StackedComparator r = new StackedComparator(defs);
    QuickSort qs = new QuickSort();
    qs.sort(r, 0, records.size());
    return new OrderIterator();
  }

  public class SortDefinition {
    boolean forward;
    boolean nullsLast;
    BasicEvaluator evaluator;

    public SortDefinition(BasicEvaluator evaluator, boolean forward, boolean nullsLast) {
      this.evaluator = evaluator;
      this.forward = forward;
      this.nullsLast = nullsLast;
    }
  }

  private class CompoundValue {
    DataValue[] values;
    RecordPointer record;

    public CompoundValue(RecordPointer record, DataValue[] values) {
      super();
      this.record = record;
      this.values = values;
    }

  }

  private class StackedComparator implements IndexedSortable {

    public StackedComparator(SortDefinition[] defs) {
    }

    @Override
    public void swap(int index0, int index1) {
      CompoundValue v = records.get(index0);
      records.set(index0, records.get(index1));
      records.set(index1, v);
    }

    @Override
    public int compare(int index0, int index1) {
      int result = 0;
      CompoundValue v1 = records.get(index0);
      CompoundValue v2 = records.get(index1);

      for (int i = 0; i < defs.length; i++) {
        boolean nullLast = defs[i].nullsLast;
        boolean asc = defs[i].forward;
        DataValue dv1 = v1.values[i];
        DataValue dv2 = v2.values[i];
        if (dv1 == DataValue.NULL_VALUE) {
          if (dv2 == DataValue.NULL_VALUE) {
            result = 0;
          } else {
            result = nullLast ? 1 : -1;
          }
        } else if (dv2 == DataValue.NULL_VALUE) {
          result = nullLast ? -1 : 1;
        } else {
          if (dv1 instanceof ComparableValue && ((ComparableValue) dv1).supportsCompare(dv2)) {
            result = ((ComparableValue) dv1).compareTo(dv2);
            if (!asc) result = -result;
          } else {
            return 0; // we break even though there may be more evaluators because we should always return the same
                      // ordering for non-comparable values no matter the compare order.
          }
        }
        if (result != 0) return result;
      }
      return result;
    }

  }

  public class OrderIterator implements RecordIterator {
    final Iterator<CompoundValue> iter;

    public OrderIterator() {
      this.iter = records.iterator();
    }

    @Override
    public NextOutcome next() {
      if (iter.hasNext()) {
        outputRecord.setRecord(iter.next().record);
        return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
      }

      return NextOutcome.NONE_LEFT;
    }

    @Override
    public ROP getParent() {
      return OrderROP.this;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return inputRecord;
    }
  }
}
