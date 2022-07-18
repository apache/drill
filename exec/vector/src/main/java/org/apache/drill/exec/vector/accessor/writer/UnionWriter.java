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
import org.apache.drill.exec.record.metadata.VariantMetadata;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter.VariantWriterListener;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.UnionWriter.UnionShim;
import org.apache.drill.exec.vector.accessor.writer.WriterEvents.ColumnWriterListener;

/**
 * Writer to a union vector.
 */
public abstract class UnionWriter implements VariantWriter, WriterEvents {

  public interface UnionShim extends WriterEvents {
    void bindWriter(UnionWriter writer);
    void setNull();
    boolean hasType(MinorType type);

    /**
     * Return an existing writer for the given type, or create a new one
     * if needed.
     *
     * @param type desired variant type
     * @return a writer for that type
     */
    ObjectWriter member(MinorType type);
    void setType(MinorType type);
    @Override
    int lastWriteIndex();
    @Override
    int rowStartIndex();
    AbstractObjectWriter addMember(ColumnMetadata colSchema);
    AbstractObjectWriter addMember(MinorType type);
    void addMember(AbstractObjectWriter colWriter);
  }

  public static class VariantObjectWriter extends AbstractObjectWriter {

    private final UnionWriterImpl writer;

    public VariantObjectWriter(UnionWriterImpl writer) {
      this.writer = writer;
    }

    @Override
    public ColumnWriter writer() { return writer; }

    @Override
    public VariantWriter variant() { return writer; }

    @Override
    public WriterEvents events() { return writer; }

    @Override
    public void dump(HierarchicalFormatter format) {
      writer.dump(format);
    }
  }

  private final ColumnMetadata schema;
  protected UnionShim shim;
  private VariantWriterListener listener;

  public UnionWriter(ColumnMetadata schema) {
    this.schema = schema;
  }

  public abstract void bindShim(UnionShim shim);
  public VariantWriterListener listener() { return listener; }
  public UnionShim shim() { return shim; }

  public void bindListener(VariantWriterListener listener) {
    this.listener = listener;
  }

  // Unions are complex: listeners should bind to the components as they
  // are created.

  @Override
  public void bindListener(ColumnWriterListener listener) { }

  @Override
  public ColumnMetadata schema() { return schema; }

  @Override
  public VariantMetadata variantSchema() { return schema.variantSchema(); }

  @Override
  public int size() { return variantSchema().size(); }

  @Override
  public ObjectType type() { return ObjectType.VARIANT; }

  @Override
  public boolean nullable() { return true; }

  @Override
  public boolean hasType(MinorType type) {
    return shim.hasType(type);
  }

  @Override
  public void setNull() {
    shim.setNull();
  }

  @Override
  public ObjectWriter memberWriter(MinorType type) {
    return shim.member(type);
  }

  @Override
  public void setType(MinorType type) {
    shim.setType(type);
  }

  @Override
  public ObjectWriter addMember(ColumnMetadata colSchema) {
    return shim.addMember(colSchema);
  }

  @Override
  public ObjectWriter addMember(MinorType type) {
    return shim.addMember(type);
  }
}
