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
package org.apache.drill.exec.physical.rowSet.model.single;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;

/**
 * Build (materialize) as set of vectors based on a provided
 * metadata schema.
 */

public class BuildVectorsFromMetadata {

  private final BufferAllocator allocator;

  public BuildVectorsFromMetadata(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public VectorContainer build(TupleMetadata schema) {
    VectorContainer container = new VectorContainer(allocator);
    for (int i = 0; i < schema.size(); i++) {
      container.add(buildVector(schema.metadata(i)));
    }

    // Build the row set from a matching triple of schema, container and
    // column models.

    container.buildSchema(SelectionVectorMode.NONE);
    return container;
  }

  private ValueVector buildVector(ColumnMetadata metadata) {
    switch (metadata.structureType()) {
    case TUPLE:
      return buildMap(metadata);
    default:
      return TypeHelper.getNewVector(metadata.schema(), allocator, null);
    }
  }

  /**
   * Build a map column including the members of the map given a map
   * column schema.
   *
   * @param schema the schema of the map column
   * @return the completed map vector column model
   */

  private AbstractMapVector buildMap(ColumnMetadata schema) {

    // Creating the map vector will create its contained vectors if we
    // give it a materialized field with children. So, instead pass a clone
    // without children so we can add the children as we add vectors.

    AbstractMapVector mapVector = (AbstractMapVector) TypeHelper.getNewVector(schema.emptySchema(), allocator, null);
    populateMap(mapVector, schema.mapSchema(), false);
    return mapVector;
  }

  /**
   * Create the contents building the model as we go.
   *
   * @param mapVector the vector to populate
   * @param mapSchema description of the map
   */

  private void populateMap(AbstractMapVector mapVector,
      TupleMetadata mapSchema, boolean inUnion) {
    for (int i = 0; i < mapSchema.size(); i++) {
      ColumnMetadata childSchema = mapSchema.metadata(i);

      // Check for union-compatible types. But, maps must be required.

      if (inUnion && ! childSchema.isMap() && childSchema.mode() == DataMode.REQUIRED) {
        throw new IllegalArgumentException("Map members in a list or union must not be non-nullable");
      }
      mapVector.putChild(childSchema.name(), buildVector(childSchema));
    }
  }
}
