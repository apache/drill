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
import org.apache.drill.exec.record.metadata.VariantMetadata;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter;
import org.apache.drill.exec.vector.accessor.writer.RepeatedListWriter;

/**
 * Build the set of writers from a defined schema. Uses the same
 * mechanism as dynamic schema: walks the schema tree adding each
 * column, then recursively adds the contents of maps and variants.
 * <p>
 * Recursion is much easier if we can go bottom-up. But, writers
 * require top-down construction.
 */

public class BuildFromSchema {

  /**
   * The shim interface provides a uniform way to add a column
   * to a parent structured column writer without the need for
   * a bunch of if-statements. Tuples (i.e. maps), UNIONs and
   * repeated LISTs all are structured (composite) columns,
   * but have slightly different semantics. This shim wraps
   * the semantics so the builder code is simpler.
   */

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
      final int index = writer.addColumn(colSchema);
      return writer.column(index);
    }
  }

  private static class UnionShim implements ParentShim {
    private final VariantWriter writer;

    public UnionShim(VariantWriter writer) {
      this.writer = writer;
    }

    @Override
    public ObjectWriter add(ColumnMetadata colSchema) {
      return writer.addMember(colSchema);
    }
  }

  private static class RepeatedListShim implements ParentShim {
    private final RepeatedListWriter writer;

    public RepeatedListShim(RepeatedListWriter writer) {
      this.writer = writer;
    }

    @Override
    public ObjectWriter add(ColumnMetadata colSchema) {
      return writer.defineElement(colSchema);
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
    final ParentShim tupleShim = new TupleShim(writer);
    for (int i = 0; i < schema.size(); i++) {
      final ColumnMetadata colSchema = schema.metadata(i);
      buildColumn(tupleShim, colSchema);
    }
  }

  private void buildColumn(ParentShim parent, ColumnMetadata colSchema) {

    if (colSchema.isMultiList()) {
      buildRepeatedList(parent, colSchema);
    } else if (colSchema.isMap()) {
      buildMap(parent, colSchema);
    } else if (isSingleList(colSchema)) {
      buildSingleList(parent, colSchema);
    } else if (colSchema.isVariant()) {
      buildVariant(parent, colSchema);
    } else {
      buildPrimitive(parent, colSchema);
    }
  }

  /**
   * Determine if the schema represents a column with a LIST type that
   * includes elements all of a single type. (Lists can be of a single
   * type (with nullable elements) or can be of unions.)
   *
   * @param colSchema schema for the column
   * @return true if the column is of type LIST with a single
   * element type
   */

  private boolean isSingleList(ColumnMetadata colSchema) {
    return colSchema.isVariant() && colSchema.isArray() && colSchema.variantSchema().isSingleType();
  }

  private void buildPrimitive(ParentShim parent, ColumnMetadata colSchema) {
    parent.add(colSchema);
  }

  private void buildMap(ParentShim parent, ColumnMetadata colSchema) {
    final ObjectWriter colWriter = parent.add(colSchema.cloneEmpty());
    expandMap(colWriter, colSchema);
  }

  private void expandMap(ObjectWriter colWriter, ColumnMetadata colSchema) {
    if (colSchema.isArray()) {
      buildTuple(colWriter.array().tuple(), colSchema.mapSchema());
    } else {
      buildTuple(colWriter.tuple(), colSchema.mapSchema());
    }
  }

  /**
   * Expand a variant column. We use the term "variant" to mean either
   * a UNION, or a LIST of UNIONs. (A LIST of UNIONs is, conceptually,
   * a repeated UNION, though it is far more complex.)
   *
   * @param parent shim object used to associate the UNION types with its
   * parent column (which is a UNION or a LIST). Since UNION and LIST are
   * far different types, the shim provides a facade that encapsulates
   * the common behavior
   * @param colSchema the schema of the variant (LIST or UNION) column
   */

  private void buildVariant(ParentShim parent, ColumnMetadata colSchema) {
    final ObjectWriter colWriter = parent.add(colSchema.cloneEmpty());
    expandVariant(colWriter, colSchema);
  }

  private void expandVariant(ObjectWriter colWriter, ColumnMetadata colSchema) {
    if (colSchema.isArray()) {
      buildUnion(colWriter.array().variant(), colSchema.variantSchema());
    } else {
      buildUnion(colWriter.variant(), colSchema.variantSchema());
    }
  }

  public void buildUnion(VariantWriter writer, VariantMetadata schema) {
    final UnionShim unionShim = new UnionShim(writer);
    for (final ColumnMetadata member : schema.members()) {
      buildColumn(unionShim, member);
    }
  }

  private void buildSingleList(ParentShim parent, ColumnMetadata colSchema) {
    final ColumnMetadata seed = colSchema.cloneEmpty();
    final ColumnMetadata subtype = colSchema.variantSchema().listSubtype();
    seed.variantSchema().addType(subtype.cloneEmpty());
    seed.variantSchema().becomeSimple();
    final ObjectWriter listWriter = parent.add(seed);
    expandColumn(listWriter, subtype);
  }

  /**
   * Expand a repeated list. The list may be multi-dimensional, meaning that
   * it may have may layers of other repeated lists before we get to the element
   * (inner-most) array.
   *
   * @param writer tuple writer for the tuple that holds the array
   * @param colSchema schema definition of the array
   */

  private void buildRepeatedList(ParentShim parent, ColumnMetadata colSchema) {
    final ColumnMetadata seed = colSchema.cloneEmpty();
    final RepeatedListWriter listWriter = (RepeatedListWriter) parent.add(seed).array();
    final ColumnMetadata elements = colSchema.childSchema();
    if (elements != null) {
      final RepeatedListShim listShim = new RepeatedListShim(listWriter);
      buildColumn(listShim, elements);
    }
  }

  /**
   * We've just built a writer for column. If the column is structured
   * (AKA "complex", meaning a map or list or array), then we need to
   * build writer for the components of the column. We do that recursively
   * here.
   *
   * @param colWriter the writer for the (possibly structured) column
   * @param colSchema the schema definition for the column
   */

  private void expandColumn(ObjectWriter colWriter, ColumnMetadata colSchema) {

    if (colSchema.isMultiList()) {
      // For completeness, should never occur.
      assert false;
    } else if (colSchema.isMap()) {
      expandMap(colWriter, colSchema);
    } else if (isSingleList(colSchema)) {
      // For completeness, should never occur.
      assert false;
    } else if (colSchema.isVariant()) {
      expandVariant(colWriter, colSchema);
    // } else {
      // Nothing to expand for primitives
    }
  }
}
