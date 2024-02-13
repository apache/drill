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

package org.apache.drill.exec.store.daffodil.schema;

import org.apache.daffodil.runtime1.api.ChoiceMetadata;
import org.apache.daffodil.runtime1.api.ComplexElementMetadata;
import org.apache.daffodil.runtime1.api.ElementMetadata;
import org.apache.daffodil.runtime1.api.MetadataHandler;
import org.apache.daffodil.runtime1.api.SequenceMetadata;
import org.apache.daffodil.runtime1.api.SimpleElementMetadata;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.MapBuilderLike;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Stack;

/**
 * This class transforms a DFDL/Daffodil schema into a Drill Schema.
 */
public class DrillDaffodilSchemaVisitor extends MetadataHandler {

  private static final Logger logger = LoggerFactory.getLogger(DrillDaffodilSchemaVisitor.class);

  /**
   * SchemaBuilder and MapBuilder share a polymorphic interface MapBuilderLike
   */
  private final SchemaBuilder builder = new SchemaBuilder();
  private final Stack<MapBuilderLike> mapBuilderStack = new Stack<>();

  private MapBuilderLike mapBuilder() {
    return mapBuilderStack.peek();
  }

  /**
   * Converts Daffodil names into appropriate Drill column names.
   * @param md Daffodil element metadata, which contains an element name.
   * @return a string usable as a Drill column name
   */
  public static String makeColumnName(ElementMetadata md) {
    return md.toQName().replace(":", "_");
  }

  /**
   * Returns a {@link TupleMetadata} representation of the DFDL schema. Should only be called after
   * the walk of the DFDL schema with this visitor has been called.
   *
   * @return A {@link TupleMetadata} representation of the DFDL schema.
   */
  public TupleMetadata getDrillSchema() {
    return builder.build();
  }

  @Override
  public void simpleElementMetadata(SimpleElementMetadata md) {
    assert (!mapBuilderStack.isEmpty());
    String colName = makeColumnName(md);
    MinorType drillType = DrillDaffodilSchemaUtils.getDrillDataType(md.jPrimType());
    if (md.isArray()) {
      mapBuilder().addArray(colName, drillType);
    } else if (md.isOptional() || md.isNillable()) {
      mapBuilder().addNullable(colName, drillType);
    } else {
      mapBuilder().add(colName, drillType);
    }
  }

  @Override
  public void startComplexElementMetadata(ComplexElementMetadata md) {
    if (mapBuilderStack.isEmpty()) {
      // root element case. The SchemaBuilder top level row is the container of the root element's children
      mapBuilderStack.push(builder);
    } else {
      // enclosed complex element case. Create a map field.
      String colName = makeColumnName(md);
      if (md.isArray()) {
        mapBuilderStack.push(mapBuilder().addMapArray(colName));
      } else {
        mapBuilderStack.push(mapBuilder().addMap(colName)); // also handles optional complex elements
      }
    }
  }

  @Override
  public void endComplexElementMetadata(ComplexElementMetadata md) {
    assert (!mapBuilderStack.isEmpty());
    mapBuilder().resume();
    mapBuilderStack.pop();
  }

  @Override
  public void startSequenceMetadata(SequenceMetadata m) {
  }

  @Override
  public void endSequenceMetadata(SequenceMetadata m) {
  }

  @Override
  public void startChoiceMetadata(ChoiceMetadata m) {
  }

  @Override
  public void endChoiceMetadata(ChoiceMetadata m) {
  }

  private void DFDLSchemaError(String s) {
    throw new RuntimeException(s);
  }
}
