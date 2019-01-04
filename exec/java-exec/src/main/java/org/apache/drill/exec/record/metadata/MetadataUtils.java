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

import java.util.List;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;

public class MetadataUtils {

  public static TupleSchema fromFields(Iterable<MaterializedField> fields) {
    TupleSchema tuple = new TupleSchema();
    for (MaterializedField field : fields) {
      tuple.add(field);
    }
    return tuple;
  }

  /**
   * Create a column metadata object that holds the given
   * {@link MaterializedField}. The type of the object will be either a
   * primitive or map column, depending on the field's type. The logic
   * here mimics the code as written, which is very messy in some places.
   *
   * @param field the materialized field to wrap
   * @return the column metadata that wraps the field
   */

  public static ColumnMetadata fromField(MaterializedField field) {
    MinorType type = field.getType().getMinorType();
    switch (type) {
    case MAP:
      return MetadataUtils.newMap(field);
    case UNION:
      if (field.getType().getMode() != DataMode.OPTIONAL) {
        throw new UnsupportedOperationException(type.name() + " type must be nullable");
      }
      return new VariantColumnMetadata(field);
    case LIST:
      switch (field.getType().getMode()) {
      case OPTIONAL:
        return new VariantColumnMetadata(field);
      case REPEATED:

        // Not a list at all, but rather the second (or third...)
        // dimension on a repeated type.

        return new RepeatedListColumnMetadata(field);
      default:

        // List of unions (or a degenerate union of a single type.)
        // Not supported in Drill.

        throw new UnsupportedOperationException(
            String.format("Unsupported mode %s for type %s",
                field.getType().getMode().name(),
                type.name()));
      }
    default:
      return new PrimitiveColumnMetadata(field);
    }
  }

  public static ColumnMetadata fromView(MaterializedField field) {
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
      tuple.add(column);
    }
    return tuple;
  }

  public static TupleMetadata fromBatchSchema(BatchSchema batchSchema) {
    TupleSchema tuple = new TupleSchema();
    for (MaterializedField field : batchSchema) {
      tuple.add(fromView(field));
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

  public static MapColumnMetadata newMap(String name, TupleMetadata schema) {
    return new MapColumnMetadata(name, DataMode.REQUIRED, (TupleSchema) schema);
  }

  public static VariantColumnMetadata newVariant(MaterializedField field, VariantSchema schema) {
    return new VariantColumnMetadata(field, schema);
  }

  public static VariantColumnMetadata newVariant(String name, DataMode cardinality) {
    switch (cardinality) {
    case OPTIONAL:
      return new VariantColumnMetadata(name, MinorType.UNION, new VariantSchema());
    case REPEATED:
      return new VariantColumnMetadata(name, MinorType.LIST, new VariantSchema());
    default:
      throw new IllegalArgumentException();
    }
  }

  public static RepeatedListColumnMetadata newRepeatedList(String name, ColumnMetadata child) {
    return new RepeatedListColumnMetadata(name, child);
  }

  public static ColumnMetadata newMapArray(String name, TupleMetadata schema) {
    return new MapColumnMetadata(name, DataMode.REPEATED, (TupleSchema) schema);
  }

  public static PrimitiveColumnMetadata newScalar(String name, MinorType type,
      DataMode mode) {
    assert type != MinorType.MAP && type != MinorType.UNION && type != MinorType.LIST;
    return new PrimitiveColumnMetadata(name, type, mode);
  }
}
