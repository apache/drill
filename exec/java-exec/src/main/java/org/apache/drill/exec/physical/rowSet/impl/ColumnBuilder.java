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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.rowSet.ProjectionSet.ColumnReadProjection;
import org.apache.drill.exec.physical.rowSet.impl.ColumnState.PrimitiveColumnState;
import org.apache.drill.exec.physical.rowSet.impl.ListState.ListVectorState;
import org.apache.drill.exec.physical.rowSet.impl.RepeatedListState.RepeatedListColumnState;
import org.apache.drill.exec.physical.rowSet.impl.RepeatedListState.RepeatedListVectorState;
import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.OffsetVectorState;
import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.SimpleVectorState;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.MapArrayState;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.MapColumnState;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.MapVectorState;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.SingleMapState;
import org.apache.drill.exec.physical.rowSet.impl.UnionState.UnionColumnState;
import org.apache.drill.exec.physical.rowSet.impl.UnionState.UnionVectorState;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.PrimitiveColumnMetadata;
import org.apache.drill.exec.record.metadata.VariantMetadata;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.writer.AbstractArrayWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractArrayWriter.ArrayObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter.TupleObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.ColumnWriterFactory;
import org.apache.drill.exec.vector.accessor.writer.EmptyListShim;
import org.apache.drill.exec.vector.accessor.writer.ListWriterImpl;
import org.apache.drill.exec.vector.accessor.writer.MapWriter;
import org.apache.drill.exec.vector.accessor.writer.RepeatedListWriter;
import org.apache.drill.exec.vector.accessor.writer.UnionWriterImpl;
import org.apache.drill.exec.vector.accessor.writer.UnionWriterImpl.VariantObjectWriter;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.apache.drill.exec.vector.complex.UnionVector;

/**
 * Algorithms for building a column given a metadata description of the column and
 * the parent context that will hold the column.
 * <p>
 * Does not support recursive column creation. For the most part, composite columns
 * (maps, map arrays, unions and lists) must start empty. Build the composite first,
 * then add its members using the writer for the column. This ensures a uniform API
 * for adding columns whether done dynamically at read time or statically at create
 * time.
 * <p>
 * The single exception is the case of a list with exactly one type: in this case
 * the list metadata must contain that one type so the code knows how to build
 * the nullable array writer for that column.
 * <p>
 * Merges the project list with the column to be built. If the column is not
 * projected (not in the list), then creates a dummy writer. Issues an error if
 * the column is projected, but the implied projection type is incompatible with
 * the actual type. (Such as trying to project an INT as x[0].)
 */
public class ColumnBuilder {

  /**
   * Implementation of the work to add a new column to this tuple given a
   * schema description of the column.
   *
   * @param parent container
   * @param columnSchema schema of the column as provided by the client
   * using the result set loader. This is the schema of the data to be
   * loaded
   * @return writer for the new column
   */
  public ColumnState buildColumn(ContainerState parent, ColumnMetadata columnSchema) {

    ColumnReadProjection colProj = parent.projectionSet().readProjection(columnSchema);
    switch (colProj.providedSchema().structureType()) {
    case TUPLE:
      return buildMap(parent, colProj);
    case VARIANT:
      // Variant: UNION or (non-repeated) LIST
      if (columnSchema.isArray()) {
        // (non-repeated) LIST (somewhat like a repeated UNION)
        return buildList(parent, colProj);
      } else {
        // (Non-repeated) UNION
        return buildUnion(parent, colProj);
      }
    case MULTI_ARRAY:
      return buildRepeatedList(parent, colProj);
    default:
      return buildPrimitive(parent, colProj);
    }
  }

  /**
   * Build a primitive column. Check if the column is projected. If not,
   * allocate a dummy writer for the column. If projected, then allocate
   * a vector, a writer, and the column state which binds the two together
   * and manages the column.
   *
   * @param parent schema of the new primitive column
   * @param colProj implied projection type for the column
   * @return column state for the new column
   */

  private ColumnState buildPrimitive(ContainerState parent, ColumnReadProjection colProj) {
    ColumnMetadata columnSchema = colProj.providedSchema();

    ValueVector vector;
    if (!colProj.isProjected()) {

      // Column is not projected. No materialized backing for the column.

      vector = null;
    } else {

      // Create the vector for the column.

      vector = parent.vectorCache().addOrGet(columnSchema.schema());

      // In permissive mode, the mode or precision of the vector may differ
      // from that requested. Update the schema to match.

      if (parent.vectorCache().isPermissive() && ! vector.getField().isEquivalent(columnSchema.schema())) {
        columnSchema = ((PrimitiveColumnMetadata) columnSchema).mergeWith(vector.getField());
      }
    }

    // Create the writer.

    final AbstractObjectWriter colWriter = ColumnWriterFactory.buildColumnWriter(
        columnSchema, colProj.conversionFactory(), vector);

    // Build the vector state which manages the vector.

    VectorState vectorState;
    if (vector == null) {
      vectorState = new NullVectorState();
    } else if (columnSchema.isArray()) {
      vectorState = new RepeatedVectorState(colWriter.array(), (RepeatedValueVector) vector);
    } else if (columnSchema.isNullable()) {
      vectorState = new NullableVectorState(
          colWriter, (NullableVector) vector);
    } else {
      vectorState = SimpleVectorState.vectorState(columnSchema,
            colWriter.events(), vector);
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
   * @param parent description of the map column
   * @param colProj implied projection type for the column
   * @return column state for the map column
   */

  private ColumnState buildMap(ContainerState parent, ColumnReadProjection colProj) {
    ColumnMetadata columnSchema = colProj.providedSchema();

    // When dynamically adding columns, must add the (empty)
    // map by itself, then add columns to the map via separate
    // calls.

    assert columnSchema.isMap();
    assert columnSchema.mapSchema().size() == 0;

    // Create the vector, vector state and writer.

    if (columnSchema.isArray()) {
      return buildMapArray(parent, colProj);
    } else {
      return buildSingleMap(parent, colProj);
    }
  }

  private ColumnState buildSingleMap(ContainerState parent, ColumnReadProjection colProj) {
    ColumnMetadata columnSchema = colProj.providedSchema();

    MapVector vector;
    VectorState vectorState;
    if (!colProj.isProjected()) {
      vector = null;
      vectorState = new NullVectorState();
    } else {

      // Don't get the map vector from the vector cache. Map vectors may
      // have content that varies from batch to batch. Only the leaf
      // vectors can be cached.

      assert columnSchema.mapSchema().isEmpty();
      vector = new MapVector(columnSchema.schema(), parent.loader().allocator(), null);
      vectorState = new MapVectorState(vector, new NullVectorState());
    }
    final TupleObjectWriter mapWriter = MapWriter.buildMap(columnSchema, vector, new ArrayList<>());
    final SingleMapState mapState = new SingleMapState(parent.loader(),
        parent.vectorCache().childCache(columnSchema.name()),
        colProj.mapProjection());
    return new MapColumnState(mapState, mapWriter, vectorState, parent.isVersioned());
  }

  private ColumnState buildMapArray(ContainerState parent, ColumnReadProjection colProj) {
    ColumnMetadata columnSchema = colProj.providedSchema();

    // Create the map's offset vector.

    RepeatedMapVector mapVector;
    UInt4Vector offsetVector;
    if (!colProj.isProjected()) {
      mapVector = null;
      offsetVector = null;
    } else {

      // Creating the map vector will create its contained vectors if we
      // give it a materialized field with children. So, instead pass a clone
      // without children so we can add them.

      final ColumnMetadata mapColSchema = columnSchema.cloneEmpty();

      // Don't get the map vector from the vector cache. Map vectors may
      // have content that varies from batch to batch. Only the leaf
      // vectors can be cached.

      assert columnSchema.mapSchema().isEmpty();
      mapVector = new RepeatedMapVector(mapColSchema.schema(),
          parent.loader().allocator(), null);
      offsetVector = mapVector.getOffsetVector();
    }

    // Create the writer using the offset vector

    final AbstractObjectWriter writer = MapWriter.buildMapArray(
        columnSchema, mapVector, new ArrayList<>());

    // Wrap the offset vector in a vector state

    VectorState offsetVectorState;
    if (!colProj.isProjected()) {
      offsetVectorState = new NullVectorState();
    } else {
      offsetVectorState = new OffsetVectorState(
          (((AbstractArrayWriter) writer.array()).offsetWriter()),
          offsetVector,
          writer.array().entry().events());
    }
    final VectorState mapVectorState = new MapVectorState(mapVector, offsetVectorState);

    // Assemble it all into the column state.

    final MapArrayState mapState = new MapArrayState(parent.loader(),
        parent.vectorCache().childCache(columnSchema.name()),
        colProj.mapProjection());
    return new MapColumnState(mapState, writer, mapVectorState, parent.isVersioned());
  }

  /**
   * Builds a union column.
   * <p>
   * The union vector type is not well supported in Drill. The idea is that
   * arbitrary operators can absorb schema changes by converting vectors to
   * unions so that an operator can handle, say, a nullable int and a varchar.
   * In practice, most operators don't support this feature. (Sort does -- but
   * does not manage memory for the union case.) In principal, union can't solve
   * the problem because ODBC and JDBC don't support unions, and it is easy
   * to envision changes that unions won't solve (int and varchar types combining
   * in a join column, say.) Still, Drill supports unions, so the code here
   * does so. Unions are fully tested in the row set writer mechanism.
   *
   * @param parent container
   * @param colProj column schema
   * @return column
   */
  private ColumnState buildUnion(ContainerState parent, ColumnReadProjection colProj) {
    ColumnMetadata columnSchema = colProj.providedSchema();
    assert columnSchema.isVariant() && ! columnSchema.isArray();

    // Create the union vector.
    // Don't get the union vector from the vector cache. Union vectors may
    // have content that varies from batch to batch. Only the leaf
    // vectors can be cached.

    assert columnSchema.variantSchema().size() == 0;
    final UnionVector vector = new UnionVector(columnSchema.schema(), parent.loader().allocator(), null);

    // Then the union writer.

    final UnionWriterImpl unionWriter = new UnionWriterImpl(columnSchema, vector, null);
    final VariantObjectWriter writer = new VariantObjectWriter(unionWriter);

    // The union vector state which manages the types vector.

    final UnionVectorState vectorState = new UnionVectorState(vector, unionWriter);

    // Create the manager for the columns within the union.

    final UnionState unionState = new UnionState(parent.loader(),
        parent.vectorCache().childCache(columnSchema.name()));

    // Bind the union state to the union writer to handle column additions.

    unionWriter.bindListener(unionState);

    // Assemble it all into a union column state.

    return new UnionColumnState(parent.loader(), writer, vectorState, unionState);
  }

  private ColumnState buildList(ContainerState parent, ColumnReadProjection colProj) {
    ColumnMetadata columnSchema = colProj.providedSchema();

    // If the list has declared a single type, and has indicated that this
    // is the only type expected, then build the list as a nullable array
    // of that type. Else, build the list as array of (a possibly empty)
    // union.

    final VariantMetadata variant = columnSchema.variantSchema();
    if (variant.isSimple()) {
      if (variant.size() == 1) {
        return buildSimpleList(parent, colProj);
      } else if (variant.size() == 0) {
        throw new IllegalArgumentException("Size of a non-expandable list can't be zero");
      }
    }
    return buildUnionList(parent, colProj);
  }

  /**
   * Create a list that is promised to only ever contain a single type (at least
   * during this write session). The list acts as a repeated vector in which each
   * element can be null. The writer is presented as an array of the single type.
   * <p>
   * List vectors (lists of optional values) are not supported in
   * Drill. The code here works up through the scan operator. But, other operators do
   * not support the <tt>ListVector</tt> type.
   *
   * @param parent the parent (tuple, union or list) that holds this list
   * @param colProj metadata description of the list which must contain
   * exactly one subtype
   * @return the column state for the list
   */

  private ColumnState buildSimpleList(ContainerState parent, ColumnReadProjection colProj) {
    ColumnMetadata columnSchema = colProj.providedSchema();

    // The variant must have the one and only type.

    assert columnSchema.variantSchema().size() == 1;
    assert columnSchema.variantSchema().isSimple();

    // Create the manager for the one and only column within the list.

    final ListState listState = new ListState(parent.loader(),
        parent.vectorCache().childCache(columnSchema.name()));

    // Create the child vector, writer and state.

    final ColumnMetadata memberSchema = columnSchema.variantSchema().listSubtype();
    final ColumnState memberState = buildColumn(listState, memberSchema);
    listState.setSubColumn(memberState);

    // Create the list vector. Contains a single type.

    final ListVector listVector = new ListVector(columnSchema.schema().cloneEmpty(),
        parent.loader().allocator(), null);
    listVector.setChildVector(memberState.vector());

    // Create the list writer: an array of the one type.

    final ListWriterImpl listWriter = new ListWriterImpl(columnSchema,
        listVector, memberState.writer());
    final AbstractObjectWriter listObjWriter = new ArrayObjectWriter(listWriter);

    // Create the list vector state that tracks the list vector lifecycle.

    final ListVectorState vectorState = new ListVectorState(listWriter,
        memberState.writer().events(), listVector);

    // Assemble it all into a union column state.

    return new UnionColumnState(parent.loader(),
        listObjWriter, vectorState, listState);
  }

  /**
   * Create a list based on a (possible) union. The list starts empty here. The client
   * can then add types as they are discovered. The list itself will transition from a
   * list of nulls (no child type), to a list of a single type, to a list of unions.
   * The writer interface will consistently present the list as a list of unions, even
   * when the list itself has no subtype or a single subtype.
   * <p>
   * List vectors (lists of unions) are not supported in
   * Drill. The code here works up through the scan operator. But, other operators do
   * not support the <tt>ListVector</tt> type.
   *
   * @param parent the parent (tuple, union or list) that holds this list
   * @param colProj metadata description of the list (must be empty of
   * subtypes)
   * @return the column state for the list
   */

  private ColumnState buildUnionList(ContainerState parent, ColumnReadProjection colProj) {
    ColumnMetadata columnSchema = colProj.providedSchema();

    // The variant must start out empty.

    assert columnSchema.variantSchema().size() == 0;

    // Create the union writer, bound to an empty list shim.

    final UnionWriterImpl unionWriter = new UnionWriterImpl(columnSchema);
    unionWriter.bindShim(new EmptyListShim());
    final VariantObjectWriter unionObjWriter = new VariantObjectWriter(unionWriter);

    // Create the list vector. Starts with the default (dummy) data
    // vector which corresponds to the empty union shim above.
    // Don't get the list vector from the vector cache. List vectors may
    // have content that varies from batch to batch. Only the leaf
    // vectors can be cached.

    final ListVector listVector = new ListVector(columnSchema.schema(),
        parent.loader().allocator(), null);

    // Create the list vector state that tracks the list vector lifecycle.

    final ListVectorState vectorState = new ListVectorState(unionWriter, listVector);

    // Create the list writer: an array of unions.

    final AbstractObjectWriter listWriter = new ArrayObjectWriter(
        new ListWriterImpl(columnSchema, listVector, unionObjWriter));

    // Create the manager for the columns within the list (which may or
    // may not be grouped into a union.)

    final ListState listState = new ListState(parent.loader(),
        parent.vectorCache().childCache(columnSchema.name()));

    // Bind the union state to the union writer to handle column additions.

    unionWriter.bindListener(listState);

    // Assemble it all into a union column state.

    return new UnionColumnState(parent.loader(),
        listWriter, vectorState, listState);
  }

  private ColumnState buildRepeatedList(ContainerState parent,
      ColumnReadProjection colProj) {
    ColumnMetadata columnSchema = colProj.providedSchema();

    assert columnSchema.type() == MinorType.LIST;
    assert columnSchema.mode() == DataMode.REPEATED;

    // The schema provided must be empty. The caller must add
    // the element type after creating the repeated writer itself.

    assert columnSchema.childSchema() == null;

    // Build the repeated vector.

    final RepeatedListVector vector = new RepeatedListVector(
        columnSchema.emptySchema(), parent.loader().allocator(), null);

    // No inner type yet. The result set loader builds the subtype
    // incrementally because it might be complex (a map or another
    // repeated list.) To start, use a dummy to avoid need for if-statements
    // everywhere.

    final ColumnMetadata dummyElementSchema = new PrimitiveColumnMetadata(
        MaterializedField.create(columnSchema.name(),
            Types.repeated(MinorType.NULL)));
    final AbstractObjectWriter dummyElement = ColumnWriterFactory.buildDummyColumnWriter(dummyElementSchema);

    // Create the list writer: an array of arrays.

    final AbstractObjectWriter arrayWriter = RepeatedListWriter.buildRepeatedList(
        columnSchema, vector, dummyElement);

    // Create the list vector state that tracks the list vector lifecycle.
    // For a repeated list, we only care about

    final RepeatedListVectorState vectorState = new RepeatedListVectorState(
        arrayWriter, vector);

    // Build the container that tracks the array contents

    final RepeatedListState listState = new RepeatedListState(
        parent.loader(),
        parent.vectorCache().childCache(columnSchema.name()));

    // Bind the list state as the list event listener.

    ((RepeatedListWriter) arrayWriter.array()).bindListener(listState);

    // Assemble it all into a column state. This state will
    // propagate events down to the (one and only) child state.

    return new RepeatedListColumnState(parent.loader(),
        arrayWriter, vectorState, listState);
  }
}
