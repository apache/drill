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

import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider.MetadataRetrieval;
import org.apache.drill.exec.physical.rowSet.model.single.BaseWriterBuilder;
import org.apache.drill.exec.physical.rowSet.model.single.BuildVectorsFromMetadata;
import org.apache.drill.exec.physical.rowSet.model.single.DirectRowIndex;
import org.apache.drill.exec.physical.rowSet.model.single.SingleSchemaInference;
import org.apache.drill.exec.physical.rowSet.model.single.VectorAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.test.rowSet.RowSet.ExtendableRowSet;
import org.apache.drill.test.rowSet.RowSetWriterImpl.WriterIndexImpl;

import java.util.Set;

/**
 * Implementation of a single row set with no indirection (selection)
 * vector.
 */

public class DirectRowSet extends AbstractSingleRowSet implements ExtendableRowSet {

  public static class RowSetWriterBuilder extends BaseWriterBuilder {

    public RowSetWriter buildWriter(DirectRowSet rowSet) {
      WriterIndexImpl index = new WriterIndexImpl();
      TupleMetadata schema = rowSet.schema();
      RowSetWriterImpl writer = new RowSetWriterImpl(rowSet, schema, index,
          buildContainerChildren(rowSet.container(),
          new MetadataRetrieval(schema)));
      return writer;
    }
  }

  private DirectRowSet(VectorContainer container, TupleMetadata schema) {
    super(container, schema);
  }

  public DirectRowSet(AbstractSingleRowSet from) {
    super(from);
  }

  public static DirectRowSet fromSchema(BufferAllocator allocator, BatchSchema schema) {
    return fromSchema(allocator, MetadataUtils.fromFields(schema));
  }

  public static DirectRowSet fromSchema(BufferAllocator allocator, TupleMetadata schema) {
    BuildVectorsFromMetadata builder = new BuildVectorsFromMetadata(allocator);
    return new DirectRowSet(builder.build(schema), schema);
  }

  public static DirectRowSet fromContainer(VectorContainer container) {
    return new DirectRowSet(container, new SingleSchemaInference().infer(container));
  }

  public static DirectRowSet fromVectorAccessible(BufferAllocator allocator, VectorAccessible va) {
    return fromContainer(toContainer(va, allocator));
  }

  private static VectorContainer toContainer(VectorAccessible va, BufferAllocator allocator) {
    VectorContainer container = VectorContainer.getTransferClone(va, allocator);
    container.buildSchema(SelectionVectorMode.NONE);
    container.setRecordCount(va.getRecordCount());
    return container;
  }

  @Override
  public void allocate(int rowCount) {
    new VectorAllocator(container()).allocate(rowCount, schema());
  }

  @Override
  public RowSetWriter writer() {
    return writer(10);
  }

  @Override
  public RowSetWriter writer(int initialRowCount) {
    if (container().hasRecordCount()) {
      throw new IllegalStateException("Row set already contains data");
    }
    allocate(initialRowCount);
    return new RowSetWriterBuilder().buildWriter(this);
  }

  @Override
  public RowSetReader reader() {
    return buildReader(new DirectRowIndex(rowCount()));
  }

  @Override
  public boolean isExtendable() { return true; }

  @Override
  public boolean isWritable() { return true; }

  @Override
  public SelectionVectorMode indirectionType() { return SelectionVectorMode.NONE; }

  @Override
  public SingleRowSet toIndirect() {
    return new IndirectRowSet(this, Sets.<Integer>newHashSet());
  }

  @Override
  public SingleRowSet toIndirect(Set<Integer> skipIndices) {
    return new IndirectRowSet(this, skipIndices);
  }

  @Override
  public SelectionVector2 getSv2() { return null; }
}
