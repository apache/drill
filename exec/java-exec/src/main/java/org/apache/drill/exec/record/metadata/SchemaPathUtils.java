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

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.MaterializedField;

public class SchemaPathUtils {

  private SchemaPathUtils() {
  }

  /**
   * Returns {@link ColumnMetadata} instance obtained from specified {@code TupleMetadata schema} which corresponds to
   * the specified column schema path.
   *
   * @param schemaPath schema path of the column which should be obtained
   * @param schema     tuple schema where column should be searched
   * @return {@link ColumnMetadata} instance which corresponds to the specified column schema path
   */
  public static ColumnMetadata getColumnMetadata(SchemaPath schemaPath, TupleMetadata schema) {
    PathSegment.NameSegment colPath = schemaPath.getUnIndexed().getRootSegment();
    ColumnMetadata colMetadata = schema.metadata(colPath.getPath());
    while (!colPath.isLastPath() && colMetadata != null) {
      if (!colMetadata.isMap()) {
        colMetadata = null;
        break;
      }
      colPath = (PathSegment.NameSegment) colPath.getChild();
      colMetadata = colMetadata.mapSchema().metadata(colPath.getPath());
    }
    return colMetadata;
  }

  /**
   * Adds column with specified schema path and type into specified {@code TupleMetadata schema}.
   *
   * @param schema     tuple schema where column should be added
   * @param schemaPath schema path of the column which should be added
   * @param type       type of the column which should be added
   */
  public static void addColumnMetadata(TupleMetadata schema, SchemaPath schemaPath, TypeProtos.MajorType type) {
    PathSegment.NameSegment colPath = schemaPath.getUnIndexed().getRootSegment();
    ColumnMetadata colMetadata;

    while (!colPath.isLastPath()) {
      colMetadata = schema.metadata(colPath.getPath());
      if (colMetadata == null) {
        colMetadata = MetadataUtils.newMap(colPath.getPath(), null);
        schema.addColumn(colMetadata);
      }
      if (!colMetadata.isMap()) {
        throw new DrillRuntimeException(String.format("Expected map, but was %s", colMetadata.majorType()));
      }

      schema = colMetadata.mapSchema();
      colPath = (PathSegment.NameSegment) colPath.getChild();
    }

    colMetadata = schema.metadata(colPath.getPath());
    if (colMetadata == null) {
      schema.addColumn(new PrimitiveColumnMetadata(MaterializedField.create(colPath.getPath(), type)));
    } else if (!colMetadata.majorType().equals(type)) {
      throw new DrillRuntimeException(String.format("Types mismatch: existing type: %s, new type: %s", colMetadata.majorType(), type));
    }
  }
}
