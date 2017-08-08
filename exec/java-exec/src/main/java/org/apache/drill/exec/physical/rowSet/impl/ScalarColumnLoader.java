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

import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnWriter;

/**
 * Represents a scalar column: one that is either at the top level of the row,
 * or is within a nested, non-repeated map. (Drill map types are really nested
 * structures AKA nested tuples.)
 */

public class ScalarColumnLoader extends AbstractScalarLoader implements ColumnLoaderImpl {

  private final AbstractColumnWriter writer;

  protected ScalarColumnLoader(WriterIndexImpl index, AbstractColumnWriter writer) {
    super(index, writer);
    this.writer = writer;
  }

  @Override
  public int writeIndex() { return writer.lastWriteIndex(); }

  @Override
  public void reset() { writer.reset(); }

  @Override
  public void resetTo(int dest) { writer.reset(dest); }

  @Override
  public void setNull() {
    assert index.legal();
    try {
      writer.setNull();
    } catch (VectorOverflowException e) {
      try {
        writer.setNull();
      } catch (VectorOverflowException e1) {
        throw new IllegalStateException(ROLLOVER_FAILED);
      }
    }
  }
}
