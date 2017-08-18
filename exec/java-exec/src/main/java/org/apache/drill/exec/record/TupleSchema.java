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
package org.apache.drill.exec.record;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;

/**
 * Defines the schema of a tuple: either the top-level row or a nested
 * "map" (really structure). A schema is a collection of columns (backed
 * by vectors in the loader itself.) Columns are accessible by name or
 * index. New columns may be added at any time; the new column takes the
 * next available index.
 */

public class TupleSchema implements TupleMetadata {

  /**
   * Abstract definition of column metadata. Allows applications to create
   * specialized forms of a column metadata object by extending from this
   * abstract class.
   * <p>
   * Note that, by design, primitive columns do not have a link to their
   * tuple parent, or their index within that parent. This allows the same
   * metadata to be shared between two views of a tuple, perhaps physical
   * and projected views. This restriction does not apply to map columns,
   * since maps (and the row itself) will, by definition, differ between
   * the two views.
   */

  public static abstract class AbstractColumnMetadata implements ColumnMetadata {

    protected MaterializedField schema;
    protected boolean projected = true;

    /**
     * Predicted number of elements per array entry. Default is
     * taken from the often hard-coded value of 10.
     */

    protected int expectedElementCount = 1;

    public AbstractColumnMetadata(MaterializedField schema) {
      this.schema = schema;
      if (isArray()) {
        expectedElementCount = DEFAULT_ARRAY_SIZE;
      }
    }

    public AbstractColumnMetadata(AbstractColumnMetadata from) {
      schema = from.schema;
      expectedElementCount = from.expectedElementCount;
    }

    protected void bind(TupleSchema parentTuple) { }

    @Override
    public MaterializedField schema() { return schema; }

    public void replaceField(MaterializedField field) {
      this.schema = field;
    }
    @Override
    public String name() { return schema().getName(); }

    @Override
    public MajorType majorType() { return schema().getType(); }

    @Override
    public MinorType type() { return schema().getType().getMinorType(); }

    @Override
    public DataMode mode() { return schema().getDataMode(); }

    @Override
    public boolean isNullable() { return mode() == DataMode.OPTIONAL; }

    @Override
    public boolean isArray() { return mode() == DataMode.REPEATED; }

    @Override
    public boolean isList() { return false; }

    @Override
    public boolean isVariableWidth() {
      MinorType type = type();
      return type == MinorType.VARCHAR || type == MinorType.VAR16CHAR || type == MinorType.VARBINARY;
    }

    @Override
    public boolean isEquivalent(ColumnMetadata other) {
      return schema().isEquivalent(other.schema());
    }

    @Override
    public int expectedWidth() { return 0; }

    @Override
    public void setExpectedWidth(int width) { }

    @Override
    public void setExpectedElementCount(int childCount) {
      // The allocation utilities don't like an array size of zero, so set to
      // 1 as the minimum. Adjusted to avoid trivial errors if the caller
      // makes an error.

      if (isArray()) {
        expectedElementCount = Math.max(1, childCount);
      }
    }

    @Override
    public int expectedElementCount() { return expectedElementCount; }

    @Override
    public void setProjected(boolean projected) {
      this.projected = projected;
    }

    @Override
    public boolean isProjected() { return projected; }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
          .append("[")
          .append(getClass().getSimpleName())
          .append(" ")
          .append(schema().toString())
          .append(",")
          .append(projected ? "" : "not ")
          .append("projected");
      if (isArray()) {
        buf.append(", cardinality: ")
           .append(expectedElementCount);
      }
      return buf
          .append("]")
          .toString();
    }

    public abstract AbstractColumnMetadata copy();
  }

  /**
   * Primitive (non-map) column. Describes non-nullable, nullable and
   * array types (which differ only in mode, but not in metadata structure.)
   */

  public static class PrimitiveColumnMetadata extends AbstractColumnMetadata {

    protected int expectedWidth;

    public PrimitiveColumnMetadata(MaterializedField schema) {
      super(schema);
      expectedWidth = TypeHelper.getSize(majorType());
      if (isVariableWidth()) {

        // The above getSize() method uses the deprecated getWidth()
        // method to get the expected VarChar size. If zero (which
        // it will be), try the revised precision field.

        int precision = majorType().getPrecision();
        if (precision > 0) {
          expectedWidth = precision;
        } else {
          // TypeHelper includes the offset vector width

          expectedWidth = expectedWidth - 4;
        }
      }
    }

    public PrimitiveColumnMetadata(PrimitiveColumnMetadata from) {
      super(from);
      expectedWidth = from.expectedWidth;
    }

    @Override
    public AbstractColumnMetadata copy() {
      return new PrimitiveColumnMetadata(this);
    }

    @Override
    public ColumnMetadata.StructureType structureType() { return ColumnMetadata.StructureType.PRIMITIVE; }

    @Override
    public TupleMetadata mapSchema() { return null; }

    @Override
    public boolean isMap() { return false; }

    @Override
    public int expectedWidth() { return expectedWidth; }

    @Override
    public void setExpectedWidth(int width) {
      // The allocation utilities don't like a width of zero, so set to
      // 1 as the minimum. Adjusted to avoid trivial errors if the caller
      // makes an error.

      if (isVariableWidth()) {
        expectedWidth = Math.max(1, width);
      }
    }

    @Override
    public ColumnMetadata cloneEmpty() {
      return new PrimitiveColumnMetadata(this);
    }
  }

  /**
   * Describes a map and repeated map. Both are tuples that have a tuple
   * schema as part of the column definition.
   */

  public static class MapColumnMetadata extends AbstractColumnMetadata {
    private TupleMetadata parentTuple;
    private final TupleSchema mapSchema;

    /**
     * Build a new map column from the field provided
     *
     * @param schema materialized field description of the map
     */

    public MapColumnMetadata(MaterializedField schema) {
      this(schema, null);
    }

    /**
     * Build a map column metadata by cloning the type information (but not
     * the children) of the materialized field provided. Use the hints
     * provided.
     *
     * @param schema the schema to use
     * @param hints metadata hints for this column
     */

    private MapColumnMetadata(MaterializedField schema, TupleSchema mapSchema) {
      super(schema);
      if (mapSchema == null) {
        this.mapSchema = new TupleSchema();
      } else {
        this.mapSchema = mapSchema;
      }
      this.mapSchema.bind(this);
    }

    @Override
    public AbstractColumnMetadata copy() {
      return new MapColumnMetadata(schema, (TupleSchema) mapSchema.copy());
    }

    @Override
    protected void bind(TupleSchema parentTuple) {
      this.parentTuple = parentTuple;
    }

    @Override
    public ColumnMetadata.StructureType structureType() { return ColumnMetadata.StructureType.TUPLE; }

    @Override
    public TupleMetadata mapSchema() { return mapSchema; }

    @Override
    public int expectedWidth() { return 0; }

    @Override
    public boolean isMap() { return true; }

    public TupleMetadata parentTuple() { return parentTuple; }

    public TupleSchema mapSchemaImpl() { return mapSchema; }

    @Override
    public ColumnMetadata cloneEmpty() {
      return new MapColumnMetadata(schema().cloneEmpty(), null);
    }
  }

  private MapColumnMetadata parentMap;
  private final TupleNameSpace<ColumnMetadata> nameSpace = new TupleNameSpace<>();

  public void bind(MapColumnMetadata parentMap) {
    this.parentMap = parentMap;
  }

  public static TupleSchema fromFields(Iterable<MaterializedField> fields) {
    TupleSchema tuple = new TupleSchema();
    for (MaterializedField field : fields) {
      tuple.add(field);
    }
    return tuple;
  }

  public TupleMetadata copy() {
    TupleMetadata tuple = new TupleSchema();
    for (ColumnMetadata md : this) {
      tuple.addColumn(((AbstractColumnMetadata) md).copy());
    }
    return tuple;
  }

  /**
   * Create a column metadata object that holds the given
   * {@link MaterializedField}. The type of the object will be either a
   * primitive or map column, depending on the field's type.
   *
   * @param field the materialized field to wrap
   * @return the column metadata that wraps the field
   */

  public static AbstractColumnMetadata fromField(MaterializedField field) {
    if (field.getType().getMinorType() == MinorType.MAP) {
      return newMap(field);
    } else {
      return new PrimitiveColumnMetadata(field);
    }
  }

  public static AbstractColumnMetadata fromView(MaterializedField field) {
    if (field.getType().getMinorType() == MinorType.MAP) {
      return new MapColumnMetadata(field, null);
    } else {
      return new PrimitiveColumnMetadata(field);
    }
  }

  /**
   * Create a tuple given the list of columns that make up the tuple.
   * Creates nested maps as needed.
   *
   * @param columns list of columns that make up the tuple
   * @return a tuple metadata object that contains the columns
   */

  public static TupleSchema fromColumns(List<ColumnMetadata> columns) {
    TupleSchema tuple = new TupleSchema();
    for (ColumnMetadata column : columns) {
      tuple.add((AbstractColumnMetadata) column);
    }
    return tuple;
  }

  /**
   * Create a column metadata object for a map column, given the
   * {@link MaterializedField} that describes the column, and a list
   * of column metadata objects that describe the columns in the map.
   *
   * @param field the materialized field that describes the map column
   * @param schema metadata that describes the tuple of columns in
   * the map
   * @return a map column metadata for the map
   */

  public static MapColumnMetadata newMap(MaterializedField field, TupleSchema schema) {
    return new MapColumnMetadata(field, schema);
  }

  public static MapColumnMetadata newMap(MaterializedField field) {
    return new MapColumnMetadata(field, fromFields(field.getChildren()));
  }

  @Override
  public ColumnMetadata add(MaterializedField field) {
    AbstractColumnMetadata md = fromField(field);
    add(md);
    return md;
  }

  public ColumnMetadata addView(MaterializedField field) {
    AbstractColumnMetadata md = fromView(field);
    add(md);
    return md;
  }

  /**
   * Add a column metadata column created by the caller. Used for specialized
   * cases beyond those handled by {@link #add(MaterializedField)}.
   *
   * @param md the custom column metadata which must have the correct
   * index set (from {@link #size()}
   */

  public void add(AbstractColumnMetadata md) {
    md.bind(this);
    nameSpace.add(md.name(), md);
    if (parentMap != null) {
      parentMap.schema.addChild(md.schema());
    }
  }

  @Override
  public int addColumn(ColumnMetadata column) {
    add((AbstractColumnMetadata) column);
    return size() - 1;
  }

  @Override
  public MaterializedField column(String name) {
    ColumnMetadata md = metadata(name);
    return md == null ? null : md.schema();
  }

  @Override
  public ColumnMetadata metadata(String name) {
    return nameSpace.get(name);
  }

  @Override
  public int index(String name) {
    return nameSpace.indexOf(name);
  }

  @Override
  public MaterializedField column(int index) {
    return metadata(index).schema();
  }

  @Override
  public ColumnMetadata metadata(int index) {
    return nameSpace.get(index);
  }

  @Override
  public MapColumnMetadata parent() { return parentMap; }

  @Override
  public int size() { return nameSpace.count(); }

  @Override
  public boolean isEmpty() { return nameSpace.count( ) == 0; }

  @Override
  public Iterator<ColumnMetadata> iterator() {
    return nameSpace.iterator();
  }

  @Override
  public boolean isEquivalent(TupleMetadata other) {
    TupleSchema otherSchema = (TupleSchema) other;
    if (nameSpace.count() != otherSchema.nameSpace.count()) {
      return false;
    }
    for (int i = 0; i < nameSpace.count(); i++) {
      if (! nameSpace.get(i).isEquivalent(otherSchema.nameSpace.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public List<MaterializedField> toFieldList() {
    List<MaterializedField> cols = new ArrayList<>();
    for (ColumnMetadata md : nameSpace) {
      cols.add(md.schema());
    }
    return cols;
  }

  public BatchSchema toBatchSchema(SelectionVectorMode svMode) {
    return new BatchSchema(svMode, toFieldList());
  }

  @Override
  public String fullName(int index) {
    return fullName(metadata(index));
  }

  @Override
  public String fullName(ColumnMetadata column) {
    String quotedName = column.name();
    if (quotedName.contains(".")) {
      quotedName = "`" + quotedName + "`";
    }
    if (isRoot()) {
      return column.name();
    } else {
      return fullName() + "." + quotedName;
    }
  }

  public String fullName() {
    if (isRoot()) {
      return "<root>";
    } else {
      return parentMap.parentTuple().fullName(parentMap);
    }
  }

  public boolean isRoot() { return parentMap == null; }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder()
        .append("[")
        .append(getClass().getSimpleName())
        .append(" ");
    boolean first = true;
    for (ColumnMetadata md : nameSpace) {
      if (! first) {
        buf.append(", ");
      }
      buf.append(md.toString());
    }
    buf.append("]");
    return buf.toString();
  }
}
