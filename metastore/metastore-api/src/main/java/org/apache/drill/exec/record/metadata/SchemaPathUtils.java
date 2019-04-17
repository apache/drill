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

import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;

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


}
