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
import org.apache.drill.exec.vector.accessor.ObjectWriter;

/**
 * Unions are overly complex. They can evolve from no type, to a single type,
 * to multiple types. The kind of vector used in these cases differ. This
 * shim acts as a facade between the writer and the underlying vector, allowing
 * the writer to remain constant while the vector (and its shim) evolves.
 */
public interface UnionShim extends WriterEvents {
  void bindWriter(UnionWriterImpl writer);
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
  boolean isProjected();

  public abstract class AbstractUnionShim implements UnionShim {

    protected final AbstractObjectWriter variants[];

    public AbstractUnionShim() {
      variants = new AbstractObjectWriter[MinorType.values().length];
    }

    public AbstractUnionShim(AbstractObjectWriter variants[]) {
      if (variants == null) {
        this.variants = new AbstractObjectWriter[MinorType.values().length];
      } else {
        this.variants = variants;
      }
    }

    @Override
    public boolean hasType(MinorType type) {
      return variants[type.ordinal()] != null;
    }

    /**
     * Performs just the work of adding a vector to the list of existing
     * variants. Called when adding a type via the writer, but also when
     * the result set loader promotes a list from single type to a union,
     * and provides this shim with the writer from the single-list shim.
     * In the latter case, the writer is already initialized and is already
     * part of the metadata for this list; so we don't want to call the
     * list's {@code addMember()} and repeat those operations.
     *
     * @param colWriter the column (type) writer to add
     */
    public void addMemberWriter(AbstractObjectWriter colWriter) {
      final MinorType type = colWriter.schema().type();
      assert variants[type.ordinal()] == null;
      variants[type.ordinal()] = colWriter;
    }
  }
}

