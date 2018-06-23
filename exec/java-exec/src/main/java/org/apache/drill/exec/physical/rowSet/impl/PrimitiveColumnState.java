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
package org.apache.drill.exec.physical.rowSet.impl;

import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.ValuesVectorState;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter.ColumnWriterListener;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractScalarWriter;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

/**
 * Primitive (non-map) column state. Handles all three cardinalities.
 * Column metadata is hosted on the writer.
 */

public class PrimitiveColumnState extends ColumnState implements ColumnWriterListener {

  public PrimitiveColumnState(ResultSetLoaderImpl resultSetLoader,
      AbstractObjectWriter colWriter,
      VectorState vectorState) {
    super(resultSetLoader, colWriter, vectorState);
    ScalarWriter scalarWriter;
    if (colWriter.type() == ObjectType.ARRAY) {
      scalarWriter = writer.array().scalar();
    } else {
      scalarWriter = writer.scalar();
    }
    ((AbstractScalarWriter) scalarWriter).bindListener(this);
  }

  public static PrimitiveColumnState newPrimitive(
      ResultSetLoaderImpl resultSetLoader,
      ValueVector vector,
      AbstractObjectWriter writer) {
    VectorState vectorState;
    if (vector == null) {
      vectorState = new NullVectorState();
    } else {
      vectorState = new ValuesVectorState(
          writer.schema(),
          (AbstractScalarWriter) writer.scalar(),
          vector);
    }
    return new PrimitiveColumnState(resultSetLoader, writer,
        vectorState);
  }

  public static PrimitiveColumnState newNullablePrimitive(
      ResultSetLoaderImpl resultSetLoader,
      ValueVector vector,
      AbstractObjectWriter writer) {
    VectorState vectorState;
    if (vector == null) {
      vectorState = new NullVectorState();
    } else {
      vectorState = new NullableVectorState(
          writer,
          (NullableVector) vector);
    }
    return new PrimitiveColumnState(resultSetLoader, writer,
        vectorState);
  }

  public static PrimitiveColumnState newPrimitiveArray(
      ResultSetLoaderImpl resultSetLoader,
      ValueVector vector,
      AbstractObjectWriter writer) {
    VectorState vectorState;
    if (vector == null) {
      vectorState = new NullVectorState();
    } else {
      vectorState = new RepeatedVectorState(writer, (RepeatedValueVector) vector);
    }
    return new PrimitiveColumnState(resultSetLoader, writer,
        vectorState);
  }

  @Override
  public void overflowed(ScalarWriter writer) {
    resultSetLoader.overflowed();
  }

  @Override
  public void dump(HierarchicalFormatter format) {
    // TODO Auto-generated method stub
  }

  @Override
  public boolean canExpand(ScalarWriter writer, int delta) {
    return resultSetLoader.canExpand(delta);
  }
}
