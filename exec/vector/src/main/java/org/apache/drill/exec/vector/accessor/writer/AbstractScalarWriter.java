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

import java.math.BigDecimal;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.UnsupportedConversionError;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.joda.time.Period;

/**
 * Column writer implementation that acts as the basis for the
 * generated, vector-specific implementations. All set methods
 * throw an exception; subclasses simply override the supported
 * method(s).
 */

public abstract class AbstractScalarWriter implements ScalarWriter, WriterEvents {

  public static class ScalarObjectWriter extends AbstractObjectWriter {

    private AbstractScalarWriter scalarWriter;

    public ScalarObjectWriter(AbstractScalarWriter scalarWriter) {
      this.scalarWriter = scalarWriter;
    }

    @Override
    public ScalarWriter scalar() { return scalarWriter; }

    @Override
    public WriterEvents events() { return scalarWriter; }

    @Override
    public void dump(HierarchicalFormatter format) {
      format
        .startObject(this)
        .attribute("scalarWriter");
      scalarWriter.dump(format);
      format.endObject();
    }
  }

  protected ColumnMetadata schema;

  /**
   * Indicates the position in the vector to write. Set via an object so that
   * all writers (within the same subtree) can agree on the write position.
   * For example, all top-level, simple columns see the same row index.
   * All columns within a repeated map see the same (inner) index, etc.
   */

  protected ColumnWriterIndex vectorIndex;

  @Override
  public ObjectType type() { return ObjectType.SCALAR; }

  public void bindSchema(ColumnMetadata schema) {
    this.schema = schema;
  }

  @Override
  public void bindIndex(ColumnWriterIndex vectorIndex) {
    this.vectorIndex = vectorIndex;
  }

  @Override
  public int rowStartIndex() {
    return vectorIndex.rowStartIndex();
  }

  @Override
  public int writeIndex() {
    return vectorIndex.vectorIndex();
  }

  @Override
  public ColumnMetadata schema() { return schema; }

  public abstract BaseDataValueVector vector();

  @Override
  public void startWrite() { }

  @Override
  public void startRow() { }

  @Override
  public void endArrayValue() { }

  @Override
  public void saveRow() { }

  protected UnsupportedConversionError conversionError(String javaType) {
    return UnsupportedConversionError.writeError(schema(), javaType);
  }

  @Override
  public void setObject(Object value) {
    if (value == null) {
      setNull();
    } else if (value instanceof Integer) {
      setInt((Integer) value);
    } else if (value instanceof Long) {
      setLong((Long) value);
    } else if (value instanceof String) {
      setString((String) value);
    } else if (value instanceof BigDecimal) {
      setDecimal((BigDecimal) value);
    } else if (value instanceof Period) {
      setPeriod((Period) value);
    } else if (value instanceof byte[]) {
      byte[] bytes = (byte[]) value;
      setBytes(bytes, bytes.length);
    } else if (value instanceof Byte) {
      setInt((Byte) value);
    } else if (value instanceof Short) {
      setInt((Short) value);
    } else if (value instanceof Double) {
      setDouble((Double) value);
    } else if (value instanceof Float) {
      setDouble((Float) value);
    } else {
      throw conversionError(value.getClass().getSimpleName());
    }
  }

  public void dump(HierarchicalFormatter format) {
    format
      .startObject(this)
      .attributeIdentity("vector", vector())
      .attribute("schema", vector().getField())
      .endObject();
  }
}
