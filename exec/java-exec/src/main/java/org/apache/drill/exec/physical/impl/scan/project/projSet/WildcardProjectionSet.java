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
package org.apache.drill.exec.physical.impl.scan.project.projSet;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;

public class WildcardProjectionSet extends AbstractProjectionSet {

  public WildcardProjectionSet(TypeConverter typeConverter) {
    super(typeConverter);
  }

  public WildcardProjectionSet(TypeConverter typeConverter, boolean isStrict) {
    super(typeConverter, isStrict);
  }

  @Override
  public boolean isProjected(String colName) { return true; }

  @Override
  public ColumnReadProjection readProjection(ColumnMetadata col) {
    if (isSpecial(col)) {
      return new UnprojectedReadColumn(col);
    }
    ColumnMetadata outputSchema = outputSchema(col);
    if (outputSchema == null) {
      if (isStrict) {
        return new UnprojectedReadColumn(col);
      }
    } else if (isSpecial(outputSchema)) {
      return new UnprojectedReadColumn(col);
    }
    if (col.isMap() || col.isDict()) {
      return new ProjectedMapColumn(col, null, outputSchema,
          new WildcardProjectionSet(childConverter(outputSchema), isStrict));

    } else {
      ColumnConversionFactory conv = conversion(col, outputSchema);
      return new ProjectedReadColumn(col, null, outputSchema, conv);
    }
  }

  @Override
  public ColumnReadProjection readDictProjection(ColumnMetadata col) {
    return readProjection(col);
  }

  // Wildcard means use whatever schema is provided by the reader,
  // so the projection itself is non-empty even if the reader has no
  // columns.

  @Override
  public boolean isEmpty() { return false; }
}
