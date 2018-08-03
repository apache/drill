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

import java.util.ArrayList;

import org.apache.drill.exec.physical.rowSet.impl.ColumnState.PrimitiveColumnState;
import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.OffsetVectorState;
import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.SimpleVectorState;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.MapArrayState;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.MapColumnState;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.MapVectorState;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.SingleMapState;
import org.apache.drill.exec.record.metadata.AbstractColumnMetadata;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.PrimitiveColumnMetadata;
import org.apache.drill.exec.record.metadata.ProjectionType;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.writer.AbstractArrayWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter.TupleObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.ColumnWriterFactory;
import org.apache.drill.exec.vector.accessor.writer.MapWriter;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;

/**
 * Algorithms for building a column given a metadata description of the column and
 * the parent context that will hold the column.
 * <p>
 * Does not support recursive column creation. For the most part, composite columns
 * (maps, map arrays, unions and lists) must start empty. Build the composite first,
 * then add its members using the writer for the column. This ensures a uniform API
 * for adding columns whether done dynamically at read time or statically at create
 * time.
 */

public class ColumnBuilder {

  private ColumnBuilder() { }

  /**
   * Implementation of the work to add a new column to this tuple given a
   * schema description of the column.
   *
   * @param columnSchema schema of the column
   * @return writer for the new column
   */

  public static ColumnState buildColumn(ContainerState parent, ColumnMetadata columnSchema) {

    // Indicate projection in the metadata.

    ((AbstractColumnMetadata) columnSchema).setProjected(
        parent.projectionType(columnSchema.name()) != ProjectionType.UNPROJECTED);

    // Build the column

    switch (columnSchema.structureType()) {
    case TUPLE:
      return buildMap(parent, columnSchema);
    default:
      return buildPrimitive(parent, columnSchema);
    }
  }

  /**
   * Build a primitive column. Check if the column is projected. If not,
   * allocate a dummy writer for the column. If projected, then allocate
   * a vector, a writer, and the column state which binds the two together
   * and manages the column.
   *
   * @param columnSchema schema of the new primitive column
   * @return column state for the new column
   */

  @SuppressWarnings("resource")
  private static ColumnState buildPrimitive(ContainerState parent, ColumnMetadata columnSchema) {
    ValueVector vector;
    if (columnSchema.isProjected()) {

      // Create the vector for the column.

      vector = parent.vectorCache().addOrGet(columnSchema.schema());

      // In permissive mode, the mode or precision of the vector may differ
      // from that requested. Update the schema to match.

      if (parent.vectorCache().isPermissive() && ! vector.getField().isEquivalent(columnSchema.schema())) {
        columnSchema = ((PrimitiveColumnMetadata) columnSchema).mergeWith(vector.getField());
      }
    } else {

      // Column is not projected. No materialized backing for the column.

      vector = null;
    }

    // Create the writer.

    AbstractObjectWriter colWriter = ColumnWriterFactory.buildColumnWriter(columnSchema, vector);

    // Build the vector state which manages the vector.

    VectorState vectorState;
    if (vector == null) {
      vectorState = new NullVectorState();
    } else if (columnSchema.isArray()) {
      vectorState = new RepeatedVectorState(colWriter.array(), (RepeatedValueVector) vector);
    } else if (columnSchema.isNullable()) {
      vectorState = new NullableVectorState(
          colWriter,
          (NullableVector) vector);
    } else {
      vectorState = SimpleVectorState.vectorState(columnSchema,
            colWriter.scalar(), vector);
    }

    // Create the column state which binds the vector and writer together.

    return new PrimitiveColumnState(parent.loader(), colWriter,
        vectorState);
  }

  /**
   * Build a new map (single or repeated) column. Except for maps nested inside
   * of unions, no map vector is created
   * here, instead we create a tuple state to hold the columns, and defer the
   * map vector (or vector container) until harvest time.
   *
   * @param columnSchema description of the map column
   * @return column state for the map column
   */

  private static ColumnState buildMap(ContainerState parent, ColumnMetadata columnSchema) {

    // When dynamically adding columns, must add the (empty)
    // map by itself, then add columns to the map via separate
    // calls.

    assert columnSchema.isMap();
    assert columnSchema.mapSchema().size() == 0;

    // Create the vector, vector state and writer.

    if (columnSchema.isArray()) {
      return buildMapArray(parent, columnSchema);
    } else {
      return buildSingleMap(parent, columnSchema);
    }
  }

  @SuppressWarnings("resource")
  private static ColumnState buildSingleMap(ContainerState parent, ColumnMetadata columnSchema) {
    MapVector vector;
    VectorState vectorState;
    if (columnSchema.isProjected()) {

      // Don't get the map vector from the vector cache. Map vectors may
      // have content that varies from batch to batch. Only the leaf
      // vectors can be cached.

      assert columnSchema.mapSchema().isEmpty();
      vector = new MapVector(columnSchema.schema(), parent.loader().allocator(), null);
      vectorState = new MapVectorState(vector, new NullVectorState());
    } else {
      vector = null;
      vectorState = new NullVectorState();
    }
    TupleObjectWriter mapWriter = MapWriter.buildMap(columnSchema,
        vector, new ArrayList<AbstractObjectWriter>());
    SingleMapState mapState = new SingleMapState(parent.loader(),
        parent.vectorCache().childCache(columnSchema.name()),
        parent.projectionSet().mapProjection(columnSchema.name()));
    return new MapColumnState(mapState, mapWriter, vectorState, parent.isVersioned());
  }

  @SuppressWarnings("resource")
  private static ColumnState buildMapArray(ContainerState parent, ColumnMetadata columnSchema) {

    // Create the map's offset vector.

    RepeatedMapVector mapVector;
    UInt4Vector offsetVector;
    if (columnSchema.isProjected()) {

      // Creating the map vector will create its contained vectors if we
      // give it a materialized field with children. So, instead pass a clone
      // without children so we can add them.

      ColumnMetadata mapColSchema = columnSchema.cloneEmpty();

      // Don't get the map vector from the vector cache. Map vectors may
      // have content that varies from batch to batch. Only the leaf
      // vectors can be cached.

      assert columnSchema.mapSchema().isEmpty();
      mapVector = new RepeatedMapVector(mapColSchema.schema(),
          parent.loader().allocator(), null);
      offsetVector = mapVector.getOffsetVector();
    } else {
      mapVector = null;
      offsetVector = null;
    }

    // Create the writer using the offset vector

    AbstractObjectWriter writer = MapWriter.buildMapArray(
        columnSchema, mapVector,
        new ArrayList<AbstractObjectWriter>());

    // Wrap the offset vector in a vector state

    VectorState offsetVectorState;
    if (columnSchema.isProjected()) {
      offsetVectorState = new OffsetVectorState(
          (((AbstractArrayWriter) writer.array()).offsetWriter()),
          offsetVector,
          writer.array().entry());
    } else {
      offsetVectorState = new NullVectorState();
    }
    VectorState mapVectorState = new MapVectorState(mapVector, offsetVectorState);

    // Assemble it all into the column state.

    MapArrayState mapState = new MapArrayState(parent.loader(),
        parent.vectorCache().childCache(columnSchema.name()),
        parent.projectionSet().mapProjection(columnSchema.name()));
    return new MapColumnState(mapState, writer, mapVectorState, parent.isVersioned());
  }
}
