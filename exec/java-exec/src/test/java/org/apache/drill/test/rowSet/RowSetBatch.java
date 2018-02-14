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
package org.apache.drill.test.rowSet;

import com.google.common.base.Preconditions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

import java.util.Iterator;

public class RowSetBatch implements RecordBatch {
  private final RowSet rowSet;

  public RowSetBatch(final RowSet rowSet) {
    this.rowSet = Preconditions.checkNotNull(rowSet);
  }

  @Override
  public FragmentContext getContext() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BatchSchema getSchema() {
    return rowSet.batchSchema();
  }

  @Override
  public int getRecordCount() {
    return rowSet.container().getRecordCount();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    if (rowSet instanceof IndirectRowSet) {
      return ((IndirectRowSet)rowSet).getSv2();
    }

    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    if (rowSet instanceof RowSet.HyperRowSet) {
      return ((RowSet.HyperRowSet)rowSet).getSv4();
    }

    throw new UnsupportedOperationException();
  }

  @Override
  public void kill(boolean sendUpstream) {
    throw new UnsupportedOperationException();
  }

  @Override
  public VectorContainer getOutgoingContainer() {
    return rowSet.container();
  }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return rowSet.container().getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    return rowSet.container().getValueAccessorById(clazz, ids);
  }

  @Override
  public IterOutcome next() {
    throw new UnsupportedOperationException();
  }

  @Override
  public WritableBatch getWritableBatch() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return rowSet.container().iterator();
  }

  @Override
  public VectorContainer getContainer() { return rowSet.container(); }
}
