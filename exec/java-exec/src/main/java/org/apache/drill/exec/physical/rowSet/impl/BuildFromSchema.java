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

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * Build the set of writers from a defined schema. Uses the same
 * mechanism as dynamic schema: walks the schema tree adding each
 * column, then recursively adding the contents of maps and variants.
 * <p>
 * Recursion is much easier if we can go bottom-up. But, writers
 * require top-down construction.
 */

public class BuildFromSchema {

  private interface ParentShim {
    ObjectWriter add(ColumnMetadata colSchema);
  }

  private static class TupleShim implements ParentShim {
    private final TupleWriter writer;

    public TupleShim(TupleWriter writer) {
      this.writer = writer;
    }

    @Override
    public ObjectWriter add(ColumnMetadata colSchema) {
      int index = writer.addColumn(colSchema);
      return writer.column(index);
    }
  }

  /**
   * When creating a schema up front, provide the schema of the desired tuple,
   * then build vectors and writers to match. Allows up-front schema definition
   * in addition to on-the-fly schema creation handled elsewhere.
   *
   * @param schema desired tuple schema to be materialized
   */

  public void buildTuple(TupleWriter writer, TupleMetadata schema) {
    ParentShim tupleShim = new TupleShim(writer);
    for (int i = 0; i < schema.size(); i++) {
      ColumnMetadata colSchema = schema.metadata(i);
      buildColumn(tupleShim, colSchema);
    }
  }

  private void buildColumn(ParentShim parent, ColumnMetadata colSchema) {

    if (colSchema.isMap()) {
      buildMap(parent, colSchema);
    } else {
      buildPrimitive(parent, colSchema);
    }
  }

  private boolean isSingleList(ColumnMetadata colSchema) {
    return colSchema.isVariant() && colSchema.isArray() && colSchema.variantSchema().isSingleType();
  }

  private void buildPrimitive(ParentShim parent, ColumnMetadata colSchema) {
    parent.add(colSchema);
  }

  private void buildMap(ParentShim parent, ColumnMetadata colSchema) {
    ObjectWriter colWriter = parent.add(colSchema.cloneEmpty());
    expandMap(colWriter, colSchema);
  }

  private void expandMap(ObjectWriter colWriter, ColumnMetadata colSchema) {
    if (colSchema.isArray()) {
      buildTuple(colWriter.array().tuple(), colSchema.mapSchema());
    } else {
      buildTuple(colWriter.tuple(), colSchema.mapSchema());
    }
  }
}
