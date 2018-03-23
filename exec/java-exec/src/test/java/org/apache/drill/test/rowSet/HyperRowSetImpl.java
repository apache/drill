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

import org.apache.drill.exec.physical.rowSet.model.MetadataProvider.MetadataRetrieval;
import org.apache.drill.exec.physical.rowSet.model.SchemaInference;
import org.apache.drill.exec.physical.rowSet.model.hyper.BaseReaderBuilder;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.test.rowSet.RowSet.HyperRowSet;

/**
 * Implements a row set wrapper around a collection of "hyper vectors."
 * A hyper-vector is a logical vector formed by a series of physical vectors
 * stacked on top of one another. To make a row set, we have a hyper-vector
 * for each column. Another way to visualize this is as a "hyper row set":
 * a stacked collection of single row sets: each column is represented by a
 * vector per row set, with each vector in a row set having the same number
 * of rows. An SV4 then provides a uniform index into the rows in the
 * hyper set. A hyper row set is read-only.
 */

public class HyperRowSetImpl extends AbstractRowSet implements HyperRowSet {

  public static class RowSetReaderBuilder extends BaseReaderBuilder {

    public RowSetReader buildReader(HyperRowSet rowSet, SelectionVector4 sv4) {
      TupleMetadata schema = rowSet.schema();
      HyperRowIndex rowIndex = new HyperRowIndex(sv4);
      return new RowSetReaderImpl(schema, rowIndex,
          buildContainerChildren(rowSet.container(),
              new MetadataRetrieval(schema)));
    }
  }

  /**
   * Selection vector that indexes into the hyper vectors.
   */

  private final SelectionVector4 sv4;

  public HyperRowSetImpl(VectorContainer container, SelectionVector4 sv4) {
    super(container, new SchemaInference().infer(container));
    this.sv4 = sv4;
  }

  public static HyperRowSet fromContainer(VectorContainer container, SelectionVector4 sv4) {
    return new HyperRowSetImpl(container, sv4);
  }

  @Override
  public boolean isExtendable() { return false; }

  @Override
  public boolean isWritable() { return false; }

  @Override
  public RowSetReader reader() {
    return new RowSetReaderBuilder().buildReader(this, sv4);
  }

  @Override
  public SelectionVectorMode indirectionType() { return SelectionVectorMode.FOUR_BYTE; }

  @Override
  public SelectionVector4 getSv4() { return sv4; }

  @Override
  public int rowCount() { return sv4.getCount(); }

  @Override
  public void clear() {
    super.clear();
    sv4.clear();
  }
}
