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
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.VariantMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractStructVector;
import org.apache.drill.exec.vector.complex.ListVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.exec.vector.complex.UnionVector;

/**
 * Build (materialize) as set of vectors based on a provided
 * metadata schema. The vectors are bundled into a
 * {@link VectorContainer} which is the end product of this
 * builder.
 * <p>
 * Vectors are built recursively by walking the tree that defines a
 * row's structure. For a classic relational tuple, the tree has just
 * a root and a set of primitives. But, once we add array (repeated),
 * variant (LIST, UNION) and tuple (MAP) columns, the tree grows
 * quite complex.
 */

public class BuildVectorsFromMetadata {

  private final BufferAllocator allocator;

  public BuildVectorsFromMetadata(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public VectorContainer build(TupleMetadata schema) {
    final VectorContainer container = new VectorContainer(allocator);
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
      return buildStruct(metadata);
    case VARIANT:
      if (metadata.isArray()) {
        return builList(metadata);
      } else {
        return buildUnion(metadata);
      }
    case MULTI_ARRAY:
      return buildRepeatedList(metadata);
    default:
      return TypeHelper.getNewVector(metadata.schema(), allocator, null);
    }
  }

  private ValueVector buildRepeatedList(ColumnMetadata metadata) {
    final RepeatedListVector listVector = new RepeatedListVector(metadata.emptySchema(), allocator, null);
    if (metadata.childSchema() != null) {
      final ValueVector child = buildVector(metadata.childSchema());
      listVector.setChildVector(child);
    }
    return listVector;
  }

  /**
   * Build a struct column including the members of the struct given a struct
   * column schema.
   *
   * @param schema the schema of the struct column
   * @return the completed struct vector column model
   */

  private AbstractStructVector buildStruct(ColumnMetadata schema) {

    // Creating the struct vector will create its contained vectors if we
    // give it a materialized field with children. So, instead pass a clone
    // without children so we can add the children as we add vectors.

    final AbstractStructVector structVector = (AbstractStructVector) TypeHelper.getNewVector(schema.emptySchema(), allocator, null);
    populateStruct(structVector, schema.mapSchema(), false);
    return structVector;
  }

  /**
   * Create the contents building the model as we go.
   *
   * @param structVector the vector to populate
   * @param mapSchema description of the struct
   */

  private void populateStruct(AbstractStructVector structVector,
                              TupleMetadata mapSchema, boolean inUnion) {
    for (int i = 0; i < mapSchema.size(); i++) {
      final ColumnMetadata childSchema = mapSchema.metadata(i);

      // Check for union-compatible types. But, maps must be required.

      if (inUnion && ! childSchema.isStruct() && childSchema.mode() == DataMode.REQUIRED) {
        throw new IllegalArgumentException("Map members in a list or union must not be non-nullable");
      }
      structVector.putChild(childSchema.name(), buildVector(childSchema));
    }
  }

  private ValueVector buildUnion(ColumnMetadata metadata) {
    final ValueVector vector = TypeHelper.getNewVector(metadata.emptySchema(), allocator, null);
    populateUnion((UnionVector) vector, metadata.variantSchema());
    return vector;
  }

  private void populateUnion(UnionVector unionVector,
      VariantMetadata variantSchema) {
    for (final MinorType type : variantSchema.types()) {
      final ValueVector childVector = unionVector.getMember(type);
      switch (type) {
      case LIST:
        populateList((ListVector) childVector,
            variantSchema.member(MinorType.LIST).variantSchema());
        break;
      case STRUCT:

        // Force the struct to require nullable or repeated types.
        // Not perfect; does not extend down to maps within maps.
        // Since this code is used in testing, not adding that complexity
        // for now.

        populateStruct((AbstractStructVector) childVector,
            variantSchema.member(MinorType.STRUCT).mapSchema(),
            true);
        break;
      default:
        // Nothing additional needed
      }
    }
  }

  private ValueVector builList(ColumnMetadata metadata) {
    final ValueVector vector = TypeHelper.getNewVector(metadata.emptySchema(), allocator, null);
    populateList((ListVector) vector, metadata.variantSchema());
    return vector;
  }

  private void populateList(ListVector vector, VariantMetadata variantSchema) {
    if (variantSchema.size() == 0) {
      return;
    } else if (variantSchema.size() == 1) {
      final ColumnMetadata subtype = variantSchema.listSubtype();
      final ValueVector childVector = buildVector(subtype);
      vector.setChildVector(childVector);
    } else {
      populateUnion(vector.fullPromoteToUnion(), variantSchema);
    }
  }
}
