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
package org.apache.drill.exec.physical.impl.protocol;

import java.util.Collections;
import java.util.Iterator;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

public class VectorContainerAccessor implements BatchAccessor {

  public static class ContainerAndSv2Accessor extends VectorContainerAccessor {

    private SelectionVector2 sv2;

    public void setSelectionVector(SelectionVector2 sv2) {
      this.sv2 = sv2;
    }

    @Override
    public SelectionVector2 getSelectionVector2() {
      return sv2;
    }
  }

  public static class ContainerAndSv4Accessor extends VectorContainerAccessor {

    private SelectionVector4 sv4;

    @Override
    public SelectionVector4 getSelectionVector4() {
      return sv4;
    }
  }

  private VectorContainer container;
  private SchemaTracker schemaTracker = new SchemaTracker();
  private int batchCount;

  /**
   * Define a schema that does not necessarily contain any data.
   * Call this to declare a schema when there are no results to
   * report.
   */

  public void setSchema(VectorContainer container) {
    this.container = container;
    if (container != null) {
      schemaTracker.trackSchema(container);
    }
  }

  /**
   * Define an output batch. Called each time a new batch is sent
   * downstream. Checks if the schema of this batch is the same as
   * that of any previous batch, and updates the schema version if
   * the schema changes. May be called with the same container
   * as the previous call, or a different one. A schema change occurs
   * unless the vectors are identical across the two containers.
   *
   * @param container the container that holds vectors to be sent
   * downstream
   */

  public void addBatch(VectorContainer container) {
    setSchema(container);
    batchCount++;
  }

  public int batchCount() { return batchCount; }

  @Override
  public BatchSchema getSchema() {
    return container == null ? null : container.getSchema();
  }

  @Override
  public int schemaVersion() { return schemaTracker.schemaVersion(); }

  @Override
  public int getRowCount() {
    return container == null ? 0 : container.getRecordCount();
  }

  @Override
  public VectorContainer getOutgoingContainer() { return container; }

  @Override
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return container.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    return container.getValueAccessorById(clazz, ids);
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(container);
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    // Throws an exception by default because containers
    // do not support selection vectors.
    return container.getSelectionVector2();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    // Throws an exception by default because containers
    // do not support selection vectors.
     return container.getSelectionVector4();
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    if (container == null) {
      return Collections.emptyIterator();
    } else {
      return container.iterator();
    }
  }

  @Override
  public void release() { container.zeroVectors(); }
}
