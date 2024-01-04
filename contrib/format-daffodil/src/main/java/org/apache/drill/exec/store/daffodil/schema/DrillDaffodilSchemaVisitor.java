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
import org.apache.daffodil.runtime1.api.InfosetSimpleElement;
import org.apache.daffodil.runtime1.api.MetadataHandler;
import org.apache.daffodil.runtime1.api.SequenceMetadata;
import org.apache.daffodil.runtime1.api.SimpleElementMetadata;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.MapBuilder;
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
   * Unfortunately, SchemaBuilder and MapBuilder, while similar, do not share a base class so we
   * have a stack of MapBuilders, and when empty we use the SchemaBuilder
   */
  private final SchemaBuilder builder = new SchemaBuilder();
  private final Stack<MapBuilder> mapBuilderStack = new Stack<>();
  /**
   * Changes to false after the very first invocation for the root element.
   */
  private boolean isOriginalRoot = true;

  public static String makeColumnName(ElementMetadata md) {
    return md.toQName().replace(":", "_");
  }

  private MapBuilder mapBuilder() {
    return mapBuilderStack.peek();
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
    assert (!isOriginalRoot);
    if (mapBuilderStack.isEmpty()) {
      simpleElementAsRowSetColumnMetadata(md);
    } else {
      simpleElementWithinComplexElementMetadata(md);
    }
  }

  private void simpleElementAsRowSetColumnMetadata(SimpleElementMetadata md) {
    assert (!isOriginalRoot);
    assert (mapBuilderStack.isEmpty());
    String colName = makeColumnName(md);
    MinorType drillType = DrillDaffodilSchemaUtils.getDrillDataType(md.primitiveType());
    //
    // below code adds to the schema builder directly, not a map builder
    //
    if (md.isArray()) {
      builder.addArray(colName, drillType);
    } else if (md.isOptional() || md.isNillable()) {
      builder.addNullable(colName, drillType);
    } else {
      builder.add(colName, drillType);
    }
  }

  private void simpleElementWithinComplexElementMetadata(SimpleElementMetadata md) {
    assert (!isOriginalRoot);
    assert (!mapBuilderStack.isEmpty());
    String colName = makeColumnName(md);
    MinorType drillType = DrillDaffodilSchemaUtils.getDrillDataType(md.primitiveType());
    //
    // The code below adds to a map builder from the stack
    //
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
    if (isOriginalRoot) {
      startComplexOriginalRootMetadata(md);
    } else if (mapBuilderStack.isEmpty()) {
      startComplexElementAsRowSetColumnMetadata(md);
    } else {
      startComplexChildElementMetadata(md);
    }
  }

  /**
   * The original root given to Drill needs to be a schema element corresponding to one row of
   * data.
   * <p>
   * Drill will call daffodil parse() to parse one such element. The children elements of this
   * element will become the column contents of the row.
   * <p>
   * So the metadata for this row, to drill, is ONLY the columns of this top level element type.
   *
   * @param md
   */
  private void startComplexOriginalRootMetadata(ComplexElementMetadata md) {
    assert (isOriginalRoot);
    assert (mapBuilderStack.isEmpty());
    isOriginalRoot = false;
    if (md instanceof InfosetSimpleElement) {
      DFDLSchemaError("Row as a simple type element is not supported.");
    }
    if (md.isArray()) {
      DFDLSchemaError("Row element must not be an array.");
    }
    //
    // We do nothing else here. Essentially the name of this top level element
    // is not relevant to drill metadata. Think of it as a table name, or a name for the
    // entire final set of rows that are the query result.
  }

  /**
   * This complex type element becomes a column of the row set which is itself a map.
   *
   * @param md
   */
  private void startComplexElementAsRowSetColumnMetadata(ComplexElementMetadata md) {
    assert (!isOriginalRoot);
    assert (mapBuilderStack.isEmpty());
    String colName = makeColumnName(md);
    //
    // This directly adds to the builder, as this complex element itself is a column
    // of the top level row
    //
    // Then it creates a map builder for recursively adding the contents.
    //
    if (md.isArray()) {
      mapBuilderStack.push(builder.addMapArray(colName));
    } else {
      mapBuilderStack.push(builder.addMap(colName)); // also handles optional complex elements
    }
  }

  private void startComplexChildElementMetadata(ComplexElementMetadata md) {
    assert (!mapBuilderStack.isEmpty());
    assert (!isOriginalRoot);
    String colName = makeColumnName(md);
    //
    // This is for non-top-level columns, but adding a field within a map.
    // That map represents a complex type element that does NOT correspond to
    // the top-level rows.
    //
    if (md.isArray()) {
      // there is no notion of optional/nullable in drill for arrays.
      // Note that our arrays are always maps. We don't have pure-value
      // simple-type arrays.
      mapBuilderStack.push(mapBuilder().addMapArray(colName));
    } else {
      // there is no concept of optional/nullable in drill for maps.
      mapBuilderStack.push(mapBuilder().addMap(colName));
    }
  }

  @Override
  public void endComplexElementMetadata(ComplexElementMetadata md) {
    // the mapBuilderStack can be empty if we had the most basic
    // kind of schema with a repeating row-set containing only
    // simple type children.
    if (!mapBuilderStack.isEmpty()) {
      if (mapBuilderStack.size() == 1) {
        mapBuilder().resumeSchema();
      } else {
        mapBuilder().resumeMap();
      }
      mapBuilderStack.pop(); // only pop if there was something on the stack.
    }
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
