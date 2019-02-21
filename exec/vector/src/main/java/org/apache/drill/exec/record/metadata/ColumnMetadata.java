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
package org.apache.drill.exec.record.metadata;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.accessor.ColumnConversionFactory;

/**
 * Metadata description of a column including names, types and structure
 * information.
 */
public interface ColumnMetadata {

  /**
   * Rough characterization of Drill types into metadata categories.
   * Various aspects of Drill's type system are very, very messy.
   * However, Drill is defined by its code, not some abstract design,
   * so the metadata system here does the best job it can to simplify
   * the messy type system while staying close to the underlying
   * implementation.
   */

  enum StructureType {

    /**
     * Primitive column (all types except List, Map and Union.)
     * Includes (one-dimensional) arrays of those types.
     */

    PRIMITIVE,

    /**
     * Map or repeated map. Also describes the row as a whole.
     */

    TUPLE,

    /**
     * Union or (non-repeated) list. (A non-repeated list is,
     * essentially, a repeated union.)
     */

    VARIANT,

    /**
     * A repeated list. A repeated list is not simply the repeated
     * form of a list, it is something else entirely. It acts as
     * a dimensional wrapper around any other type (except list)
     * and adds a non-nullable extra dimension. Hence, this type is
     * for 2D+ arrays.
     * <p>
     * In theory, a 2D list of, say, INT would be an INT column, but
     * repeated in to dimensions. Alas, that is not how it is. Also,
     * if we have a separate category for 2D lists, we should have
     * a separate category for 1D lists. But, again, that is not how
     * the code has evolved.
     */

    MULTI_ARRAY
  }

  int DEFAULT_ARRAY_SIZE = 10;

  StructureType structureType();

  /**
   * Schema for <tt>TUPLE</tt> columns.
   *
   * @return the tuple schema
   */

  TupleMetadata mapSchema();

  /**
   * Schema for <tt>VARIANT</tt> columns.
   *
   * @return the variant schema
   */

  VariantMetadata variantSchema();

  /**
   * Schema of inner dimension for <tt>MULTI_ARRAY<tt> columns.
   * If an array is 3D, the outer column represents all 3 dimensions.
   * <tt>outer.childSchema()</tt> gives another <tt>MULTI_ARRAY</tt>
   * for the inner 2D array.
   * <tt>outer.childSchema().childSchema()</tt> gives a column
   * of some other type (but repeated) for the 1D array.
   * <p>
   * Sorry for the mess, but it is how the code works and we are not
   * in a position to revisit data type fundamentals.
   *
   * @return the description of the (n-1) st dimension.
   */

  ColumnMetadata childSchema();
  MaterializedField schema();
  MaterializedField emptySchema();
  String name();
  MinorType type();
  MajorType majorType();
  DataMode mode();
  int dimensions();
  boolean isNullable();
  boolean isArray();
  boolean isVariableWidth();
  boolean isMap();
  boolean isVariant();

  /**
   * Determine if the schema represents a column with a LIST type with
   * UNION elements. (Lists can be of a single
   * type (with nullable elements) or can be of unions.)
   *
   * @return true if the column is of type LIST of UNIONs
   */

  boolean isMultiList();

  /**
   * Report whether one column is equivalent to another. Columns are equivalent
   * if they have the same name, type and structure (ignoring internal structure
   * such as offset vectors.)
   */

  boolean isEquivalent(ColumnMetadata other);

  /**
   * For variable-width columns, specify the expected column width to be used
   * when allocating a new vector. Does nothing for fixed-width columns.
   *
   * @param width the expected column width
   */

  void setExpectedWidth(int width);

  /**
   * Get the expected width for a column. This is the actual width for fixed-
   * width columns, the specified width (defaulting to 50) for variable-width
   * columns.
   * @return the expected column width of the each data value. Does not include
   * "overhead" space such as for the null-value vector or offset vector
   */

  int expectedWidth();

  /**
   * For an array column, specify the expected average array cardinality.
   * Ignored for non-array columns. Used when allocating new vectors.
   *
   * @param childCount the expected average array cardinality. Defaults to
   * 1 for non-array columns, 10 for array columns
   */

  void setExpectedElementCount(int childCount);

  /**
   * Returns the expected array cardinality for array columns, or 1 for
   * non-array columns.
   *
   * @return the expected value cardinality per value (per-row for top-level
   * columns, per array element for arrays within lists)
   */

  int expectedElementCount();

  /**
   * Set the default value to use for filling a vector when no real data is
   * available, such as for columns added in new files but which does not
   * exist in existing files. The "default default" is null, which works
   * only for nullable columns.
   *
   * @param value column value, represented as a Java object, acceptable
   * to the {@link ColumnWriter#setObject()} method for this column's writer.
   */
  void setDefaultValue(Object value);

  /**
   * Returns the default value for this column.
   *
   * @return the default value, or null if no default value has been set
   */
  Object defaultValue();

  /**
   * Set the factory for an optional shim writer that translates from the type of
   * data available to the code that creates the vectors on the one hand,
   * and the actual type of the column on the other. For example, a shim
   * might parse a string form of a date into the form stored in vectors.
   * <p>
   * The shim must write to the base vector for this column using one of
   * the supported base writer "set" methods.
   * <p>
   * The default is to use the "natural" type: that is, to insert no
   * conversion shim.
   */
  void setTypeConverter(ColumnConversionFactory factory);

  /**
   * Returns the type conversion shim for this column.
   *
   * @return the type conversion factory, or null if none is set
   */
  ColumnConversionFactory typeConverter();

  /**
   * Create an empty version of this column. If the column is a scalar,
   * produces a simple copy. If a map, produces a clone without child
   * columns.
   *
   * @return empty clone of this column
   */

  ColumnMetadata cloneEmpty();

  /**
   * Reports whether, in this context, the column is projected outside
   * of the context. (That is, whether the column is backed by an actual
   * value vector.)
   */

  boolean isProjected();
  void setProjected(boolean projected);

  int precision();
  int scale();

  void bind(TupleMetadata parentTuple);

  ColumnMetadata copy();

  /**
   * Converts type metadata into string representation
   * accepted by the table schema parser.
   *
   * @return type metadata string representation
   */
  String typeString();

  /**
   * Converts column metadata into string representation
   * accepted by the table schema parser.
   *
   * @return column metadata string representation
   */
  String columnString();

}
