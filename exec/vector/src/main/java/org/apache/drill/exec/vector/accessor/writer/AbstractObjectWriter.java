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

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriter;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.VariantWriter;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;

/**
 * Abstract base class for the object layer in writers. This class acts
 * as the glue between a column and the data type of that column, per the
 * JSON model which Drill uses. This base class provides stubs for most
 * methods so that type-specific subclasses can simply fill in the bits
 * needed for that particular class.
 */

public abstract class AbstractObjectWriter implements ObjectWriter {

  @Override
  public ColumnMetadata schema() { return baseWriter().schema(); }

  @Override
  public ScalarWriter scalar() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TupleWriter tuple() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrayWriter array() {
    throw new UnsupportedOperationException();
  }

  @Override
  public VariantWriter variant() {
    throw new UnsupportedOperationException();
  }

  public abstract WriterEvents events();

  public ColumnWriter baseWriter() {
    return (ColumnWriter) events();
  }

  @Override
  public ObjectType type() { return baseWriter().type(); }

  @Override
  public boolean nullable() { return baseWriter().nullable(); }

  @Override
  public void setNull() {
    baseWriter().setNull();
  }

  @Override
  public void setObject(Object value) {
    baseWriter().setObject(value);
  }

  public abstract void dump(HierarchicalFormatter format);

  @Override
  public int rowStartIndex() {
    return baseWriter().rowStartIndex();
  }

  @Override
  public int lastWriteIndex() {
    return baseWriter().lastWriteIndex();
  }

  @Override
  public int writeIndex() {
    return baseWriter().writeIndex();
  }
}
