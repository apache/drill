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
package org.apache.drill.exec.physical.rowSet.model.hyper;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider;
import org.apache.drill.exec.physical.rowSet.model.MetadataProvider.VectorDescrip;
import org.apache.drill.exec.physical.rowSet.model.ReaderIndex;
import org.apache.drill.exec.record.HyperVectorWrapper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.impl.AccessorUtilities;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.ColumnReaderFactory;
import org.apache.drill.exec.vector.accessor.reader.MapReader;
import org.apache.drill.exec.vector.accessor.reader.ObjectArrayReader;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor;
import org.apache.drill.exec.vector.complex.AbstractMapVector;

public abstract class BaseReaderBuilder {

  /**
   * Read-only row index into the hyper row set with batch and index
   * values mapping via an SV4.
   */

  public static class HyperRowIndex extends ReaderIndex {

    private final SelectionVector4 sv4;

    public HyperRowIndex(SelectionVector4 sv4) {
      super(sv4.getCount());
      this.sv4 = sv4;
    }

    @Override
    public int vectorIndex() {
      return AccessorUtilities.sv4Index(sv4.get(rowIndex));
    }

    @Override
    public int batchIndex( ) {
      return AccessorUtilities.sv4Batch(sv4.get(rowIndex));
    }
  }

  /**
   * Vector accessor used by the column accessors to obtain the vector for
   * each column value. That is, position 0 might be batch 4, index 3,
   * while position 1 might be batch 1, index 7, and so on.
   */

  public static class HyperVectorAccessor implements VectorAccessor {

    private final ValueVector[] vectors;
    private ColumnReaderIndex rowIndex;

    public HyperVectorAccessor(VectorWrapper<?> vw) {
      vectors = vw.getValueVectors();
    }

    @Override
    public void bind(ColumnReaderIndex index) {
      rowIndex = index;
    }

    @Override
    public ValueVector vector() {
      return vectors[rowIndex.batchIndex()];
    }
  }


  protected AbstractObjectReader[] buildContainerChildren(
      VectorContainer container, MetadataProvider mdProvider) {
    List<AbstractObjectReader> readers = new ArrayList<>();
    for (int i = 0; i < container.getNumberOfColumns(); i++) {
      VectorWrapper<?> vw = container.getValueVector(i);
      VectorDescrip descrip = new VectorDescrip(mdProvider, i, vw.getField());
      readers.add(buildVectorReader(vw, descrip));
    }
    return readers.toArray(new AbstractObjectReader[readers.size()]);
  }

  @SuppressWarnings("unchecked")
  private AbstractObjectReader buildVectorReader(VectorWrapper<?> vw, VectorDescrip descrip) {
    MajorType type = vw.getField().getType();
    if (type.getMinorType() == MinorType.MAP) {
      if (type.getMode() == DataMode.REPEATED) {
        return buildMapArrayReader((HyperVectorWrapper<? extends AbstractMapVector>) vw, descrip);
      } else {
        return buildMapReader((HyperVectorWrapper<? extends AbstractMapVector>) vw, descrip);
      }
    } else {
      return buildPrimitiveReader(vw, descrip);
    }
  }

  private AbstractObjectReader buildMapArrayReader(HyperVectorWrapper<? extends AbstractMapVector> vectors, VectorDescrip descrip) {
    AbstractObjectReader mapReader = MapReader.build(descrip.metadata, buildMap(vectors, descrip));
    return ObjectArrayReader.build(new HyperVectorAccessor(vectors), mapReader);
  }

  private AbstractObjectReader buildMapReader(HyperVectorWrapper<? extends AbstractMapVector> vectors, VectorDescrip descrip) {
    return MapReader.build(descrip.metadata, buildMap(vectors, descrip));
  }

  private AbstractObjectReader buildPrimitiveReader(VectorWrapper<?> vw, VectorDescrip descrip) {
    return ColumnReaderFactory.buildColumnReader(
        vw.getField().getType(), new HyperVectorAccessor(vw));
  }

  private List<AbstractObjectReader> buildMap(HyperVectorWrapper<? extends AbstractMapVector> vectors, VectorDescrip descrip) {
    List<AbstractObjectReader> readers = new ArrayList<>();
    MetadataProvider provider = descrip.parent.childProvider(descrip.metadata);
    MaterializedField mapField = vectors.getField();
    for (int i = 0; i < mapField.getChildren().size(); i++) {
      HyperVectorWrapper<? extends ValueVector> child = (HyperVectorWrapper<? extends ValueVector>) vectors.getChildWrapper(new int[] {i});
      VectorDescrip childDescrip = new VectorDescrip(provider, i, child.getField());
      readers.add(buildVectorReader(child, childDescrip));
      i++;
    }
    return readers;
  }
}
