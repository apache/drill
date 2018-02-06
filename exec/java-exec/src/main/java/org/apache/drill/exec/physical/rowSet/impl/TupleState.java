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
import java.util.List;

import org.apache.drill.exec.physical.rowSet.impl.ColumnState.BaseMapColumnState;
import org.apache.drill.exec.physical.rowSet.impl.ColumnState.MapArrayColumnState;
import org.apache.drill.exec.physical.rowSet.impl.ColumnState.MapColumnState;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.AbstractColumnMetadata;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter.TupleWriterListener;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractTupleWriter;
import org.apache.drill.exec.vector.accessor.writer.ColumnWriterFactory;

/**
 * Represents the loader state for a tuple: a row or a map. This is "state" in
 * the sense of variables that are carried along with each tuple. Handles
 * write-time issues such as defining new columns, allocating memory, handling
 * overflow, assembling the output version of the map, and so on. Each
 * row and map in the result set has a tuple state instances associated
 * with it.
 * <p>
 * Here, by "tuple" we mean a container of vectors, each of which holds
 * a variety of values. So, the "tuple" here is structural, not a specific
 * set of values, but rather the collection of vectors that hold tuple
 * values.
 */

public abstract class TupleState implements TupleWriterListener {

  /**
   * Handles the details of the top-level tuple, the data row itself.
   * Note that by "row" we mean the set of vectors that define the
   * set of rows.
   */

  public static class RowState extends TupleState {

    /**
     * The row-level writer for stepping through rows as they are written,
     * and for accessing top-level columns.
     */

    private final RowSetLoaderImpl writer;

    public RowState(ResultSetLoaderImpl rsLoader) {
      super(rsLoader, rsLoader.projectionSet);
      writer = new RowSetLoaderImpl(rsLoader, schema);
      writer.bindListener(this);
    }

    public RowSetLoaderImpl rootWriter() { return writer; }

    @Override
    public AbstractTupleWriter writer() { return writer; }

    @Override
    public int innerCardinality() { return resultSetLoader.targetRowCount();}
  }

  /**
   * Represents a tuple defined as a Drill map: single or repeated. Note that
   * the map vector does not exist here; it is assembled only when "harvesting"
   * a batch. This design supports the obscure case in which a new column
   * is added during an overflow row, so exists within this abstraction,
   * but is not published to the map that makes up the output.
   */

  public static class MapState extends TupleState {

    protected final BaseMapColumnState mapColumnState;
    protected int outerCardinality;

    public MapState(ResultSetLoaderImpl rsLoader,
        BaseMapColumnState mapColumnState,
        ProjectionSet projectionSet) {
      super(rsLoader, projectionSet);
      this.mapColumnState = mapColumnState;
      mapColumnState.writer().bindListener(this);
    }

    /**
     * Return the tuple writer for the map. If this is a single
     * map, then it is the writer itself. If this is a map array,
     * then the tuple is nested inside the array.
     */

    @Override
    public AbstractTupleWriter writer() {
      AbstractObjectWriter objWriter = mapColumnState.writer();
      TupleWriter tupleWriter;
      if (objWriter.type() == ObjectType.ARRAY) {
        tupleWriter = objWriter.array().tuple();
      } else {
        tupleWriter = objWriter.tuple();
      }
      return (AbstractTupleWriter) tupleWriter;
    }

    /**
     * In order to allocate the correct-sized vectors, the map must know
     * its member cardinality: the number of elements in each row. This
     * is 1 for a single map, but may be any number for a map array. Then,
     * this value is recursively pushed downward to compute the cardinality
     * of lists of maps that contains lists of maps, and so on.
     */

    @Override
    public void updateCardinality(int outerCardinality) {
      this.outerCardinality = outerCardinality;
      super.updateCardinality(outerCardinality);
    }

    @Override
    public int innerCardinality() {
      return outerCardinality * mapColumnState.schema().expectedElementCount();
    }

    @Override
    public void dump(HierarchicalFormatter format) {
      format
        .startObject(this)
        .attribute("column", mapColumnState.schema().name())
        .attribute("cardinality", outerCardinality)
        .endObject();
    }
  }

  protected final ResultSetLoaderImpl resultSetLoader;
  protected final List<ColumnState> columns = new ArrayList<>();
  protected final TupleSchema schema = new TupleSchema();
  protected final ProjectionSet projectionSet;

  protected TupleState(ResultSetLoaderImpl rsLoader, ProjectionSet projectionSet) {
    this.resultSetLoader = rsLoader;
    this.projectionSet = projectionSet;
  }

  public abstract int innerCardinality();

  /**
   * Returns an ordered set of the columns which make up the tuple.
   * Column order is the same as that defined by the map's schema,
   * to allow indexed access. New columns always appear at the end
   * of the list to preserve indexes.
   *
   * @return ordered list of column states for the columns within
   * this tuple
   */

  public List<ColumnState> columns() { return columns; }

  public TupleMetadata schema() { return writer().schema(); }

  public abstract AbstractTupleWriter writer();

  @Override
  public ObjectWriter addColumn(TupleWriter tupleWriter, MaterializedField column) {
    return addColumn(tupleWriter, MetadataUtils.fromField(column));
  }

  @Override
  public ObjectWriter addColumn(TupleWriter tupleWriter, ColumnMetadata columnSchema) {

    // Verify name is not a (possibly case insensitive) duplicate.

    TupleMetadata tupleSchema = schema();
    String colName = columnSchema.name();
    if (tupleSchema.column(colName) != null) {
      throw new IllegalArgumentException("Duplicate column: " + colName);
    }

    return addColumn(columnSchema);
  }

  /**
   * Implementation of the work to add a new column to this tuple given a
   * schema description of the column.
   *
   * @param columnSchema schema of the column
   * @return writer for the new column
   */

  private AbstractObjectWriter addColumn(ColumnMetadata columnSchema) {

    // Indicate projection in the metadata.

    ((AbstractColumnMetadata) columnSchema).setProjected(
        projectionSet.isProjected(columnSchema.name()));

    // Build the column

    ColumnState colState;
    if (columnSchema.isMap()) {
      colState = buildMap(columnSchema);
    } else {
      colState = buildPrimitive(columnSchema);
    }
    columns.add(colState);
    colState.updateCardinality(innerCardinality());
    colState.allocateVectors();
    return colState.writer();
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
  private ColumnState buildPrimitive(ColumnMetadata columnSchema) {
    ValueVector vector;
    if (columnSchema.isProjected()) {

      // Create the vector for the column.

      vector = resultSetLoader.vectorCache().addOrGet(columnSchema.schema());
    } else {

      // Column is not projected. No materialized backing for the column.

      vector = null;
    }

    // Create the writer. Will be returned to the tuple writer.

    AbstractObjectWriter colWriter = ColumnWriterFactory.buildColumnWriter(columnSchema, vector);

    if (columnSchema.isArray()) {
      return PrimitiveColumnState.newPrimitiveArray(resultSetLoader, vector, colWriter);
    } else {
      return PrimitiveColumnState.newPrimitive(resultSetLoader, vector, colWriter);
    }
  }

  /**
   * Build a new map (single or repeated) column. No map vector is created
   * here, instead we create a tuple state to hold the columns, and defer the
   * map vector (or vector container) until harvest time.
   *
   * @param columnSchema description of the map column
   * @return column state for the map column
   */

  private ColumnState buildMap(ColumnMetadata columnSchema) {

    // When dynamically adding columns, must add the (empty)
    // map by itself, then add columns to the map via separate
    // calls.

    assert columnSchema.isMap();
    assert columnSchema.mapSchema().size() == 0;

    // Create the writer. Will be returned to the tuple writer.

    ProjectionSet childProjection = projectionSet.mapProjection(columnSchema.name());
    if (columnSchema.isArray()) {
      return MapArrayColumnState.build(resultSetLoader,
          columnSchema,
          childProjection);
    } else {
      return new MapColumnState(resultSetLoader,
          columnSchema,
          childProjection);
    }
  }

  /**
   * When creating a schema up front, provide the schema of the desired tuple,
   * then build vectors and writers to match. Allows up-front schema definition
   * in addition to on-the-fly schema creation handled elsewhere.
   *
   * @param schema desired tuple schema to be materialized
   */

  public void buildSchema(TupleMetadata schema) {
    for (int i = 0; i < schema.size(); i++) {
      ColumnMetadata colSchema = schema.metadata(i);
      AbstractObjectWriter colWriter;
      if (colSchema.isMap()) {
        colWriter = addColumn(colSchema.cloneEmpty());
        BaseMapColumnState mapColState = (BaseMapColumnState) columns.get(columns.size() - 1);
        mapColState.mapState().buildSchema(colSchema.mapSchema());
      } else {
        colWriter = addColumn(colSchema);
      }
      writer().addColumnWriter(colWriter);
    }
  }

  public void updateCardinality(int cardinality) {
    for (ColumnState colState : columns) {
      colState.updateCardinality(cardinality);
    }
  }

  /**
   * A column within the row batch overflowed. Prepare to absorb the rest of the
   * in-flight row by rolling values over to a new vector, saving the complete
   * vector for later. This column could have a value for the overflow row, or
   * for some previous row, depending on exactly when and where the overflow
   * occurs.
   */

  public void rollover() {
    for (ColumnState colState : columns) {
      colState.rollover();
    }
  }

  /**
   * Writing of a row batch is complete, and an overflow occurred. Prepare the
   * vector for harvesting to send downstream. Set aside the look-ahead vector
   * and put the full vector buffer back into the active vector.
   */

  public void harvestWithLookAhead() {
    for (ColumnState colState : columns) {
      colState.harvestWithLookAhead();
    }
  }

  /**
   * Start a new batch by shifting the overflow buffers back into the main
   * write vectors and updating the writers.
   */

  public void startBatch() {
    for (ColumnState colState : columns) {
      colState.startBatch();
    }
  }

  /**
   * Clean up state (such as backup vectors) associated with the state
   * for each vector.
   */

  public void close() {
    for (ColumnState colState : columns) {
      colState.close();
    }
  }

  public void dump(HierarchicalFormatter format) {
    format
      .startObject(this)
      .attributeArray("columns");
    for (int i = 0; i < columns.size(); i++) {
      format.element(i);
      columns.get(i).dump(format);
    }
    format
      .endArray()
      .endObject();
  }
}
