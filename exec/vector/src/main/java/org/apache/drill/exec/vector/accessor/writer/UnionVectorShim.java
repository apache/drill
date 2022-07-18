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
package org.apache.drill.exec.vector.accessor.writer;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter.VariantWriterListener;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.AbstractFixedWidthWriter.BaseFixedWidthWriter;
import org.apache.drill.exec.vector.accessor.writer.UnionShim.AbstractUnionShim;
import org.apache.drill.exec.vector.complex.UnionVector;

/**
 * Lists can operate in three modes: no type, one type or many
 * types (that is, a list of unions.) This shim implements the
 * variant writer when the backing vector is a union or a list
 * backed by a union. It is a distinct class so that a List
 * writer can have a uniform variant-style interface even as the
 * list itself evolves from no type, to a single type and to
 * a union.
 */
public class UnionVectorShim extends AbstractUnionShim {

  static class DefaultListener implements VariantWriterListener {

    private final UnionVectorShim shim;

    private DefaultListener(UnionVectorShim shim) {
      this.shim = shim;
    }

    @Override
    public ObjectWriter addType(MinorType type) {

      // This vector layer does not have visibility to the implementation
      // classes for metadata. Use the methods provided to add the type
      // which will create the member metadata. This means that the type
      // will already be in the variant schema by the time we add the
      // writer to the variant writer in a few steps from now.
      final ValueVector memberVector = shim.vector.getMember(type);
      final ColumnMetadata memberSchema = shim.writer.variantSchema().addType(type);
      return ColumnWriterFactory.buildColumnWriter(memberSchema, memberVector);
    }

    @Override
    public ObjectWriter addMember(ColumnMetadata schema) {
      throw new UnsupportedOperationException();
    }
  }

  private final UnionVector vector;
  private UnionWriterImpl writer;

  /**
   * Writer for the type vector associated with the union. The type vector
   * says which union member holds the value for each row. The type vector
   * can also indicate if the value is null.
   */
  private final BaseScalarWriter typeWriter;

  public UnionVectorShim(UnionVector vector) {
    this.vector = vector;
    typeWriter = ColumnWriterFactory.newWriter(vector.getTypeVector());
  }

  public UnionVectorShim(UnionVector vector,
      AbstractObjectWriter variants[]) {
    super(variants);
    this.vector = vector;
    typeWriter = ColumnWriterFactory.newWriter(vector.getTypeVector());
  }

  @Override
  public void bindWriter(UnionWriterImpl writer) {
    this.writer = writer;
    final ColumnWriterIndex index = writer.index();
    if (index != null) {
      bindIndex(index);
    }
  }

  @Override
  public void bindIndex(ColumnWriterIndex index) {
    typeWriter.bindIndex(index);
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().bindIndex(index);
      }
    }
  }

  // Unions are complex: the listener should bind to the individual components
  // as they are created.
  @Override
  public void bindListener(ColumnWriterListener listener) { }

  @Override
  public void setNull() {

    // Not really necessary: the default value is 0.
    // This lets a caller change its mind after setting a
    // value.
    typeWriter.setInt(UnionVector.NULL_MARKER);
  }

  @Override
  public ObjectWriter member(MinorType type) {
    final AbstractObjectWriter colWriter = variants[type.ordinal()];
    if (colWriter != null) {
      return colWriter;
    }
    if (writer.listener() == null) {
      writer.bindListener(new DefaultListener(this));
    }
    return addMember(type);
  }

  @Override
  public void setType(MinorType type) {
    typeWriter.setInt(type.getNumber());
  }

  @Override
  public AbstractObjectWriter addMember(ColumnMetadata schema) {
    final AbstractObjectWriter colWriter = (AbstractObjectWriter) writer.listener().addMember(schema);
    addMember(colWriter);
    return colWriter;
  }

  @Override
  public AbstractObjectWriter addMember(MinorType type) {
    final AbstractObjectWriter colWriter = (AbstractObjectWriter) writer.listener().addType(type);
    addMember(colWriter);
    return colWriter;
  }

  /**
   * Add a column writer to an existing union writer. Used for implementations
   * that support "live" schema evolution: column discovery while writing.
   * The corresponding metadata must already have been added to the schema.
   *
   * @param colWriter the column writer to add
   */
  @Override
  public void addMember(AbstractObjectWriter colWriter) {
    addMemberWriter(colWriter);
    writer.addMember(colWriter);
  }

  @Override
  public void startWrite() {
    typeWriter.startWrite();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().startWrite();
      }
    }
  }

  @Override
  public void startRow() {
    typeWriter.startRow();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().startRow();
      }
    }
  }

  @Override
  public void endArrayValue() {
    typeWriter.endArrayValue();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().endArrayValue();
      }
    }
  }

  @Override
  public void restartRow() {
    typeWriter.restartRow();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().restartRow();
      }
    }
  }

  @Override
  public void saveRow() {
    typeWriter.saveRow();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().saveRow();
      }
    }
   }

  @Override
  public void preRollover() {
    typeWriter.preRollover();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().preRollover();
      }
    }
  }

  @Override
  public void postRollover() {
    typeWriter.postRollover();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().postRollover();
      }
    }
  }

  @Override
  public void endWrite() {
    typeWriter.endWrite();
    for (int i = 0; i < variants.length; i++) {
      if (variants[i] != null) {
        variants[i].events().endWrite();
      }
    }
  }


  /**
   * Return the writer for the types vector. To be used only by the row set
   * loader overflow logic; never by the application (which is why the method
   * is not defined in the interface.)
   *
   * @return the writer for the types vector
   */
  public AbstractScalarWriterImpl typeWriter() { return typeWriter; }

  @Override
  public int writeIndex() { return typeWriter.writeIndex(); }

  @Override
  public int lastWriteIndex() { return typeWriter.lastWriteIndex(); }

  @Override
  public int rowStartIndex() { return typeWriter.rowStartIndex(); }

  /**
   * When promoting the list to a union, the union's
   * type vector was initialized for any rows written thus
   * far. Tell the type writer that those positions have been
   * written so that they are not zero-filled.
   */
  public void initTypeIndex(int typeFillCount) {
    ((BaseFixedWidthWriter) typeWriter).setLastWriteIndex(typeFillCount);
  }

  @Override
  public void dump(HierarchicalFormatter format) {
    format.startObject(this).attribute("typeWriter");
    typeWriter.dump(format);
    format.endObject();
  }
}