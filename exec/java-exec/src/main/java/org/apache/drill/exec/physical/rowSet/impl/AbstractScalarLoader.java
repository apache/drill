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

import java.math.BigDecimal;

import org.apache.drill.exec.physical.rowSet.ArrayLoader;
import org.apache.drill.exec.physical.rowSet.ScalarLoader;
import org.apache.drill.exec.physical.rowSet.TupleLoader;
import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.ColumnAccessor.ValueType;
import org.joda.time.Period;

/**
 * Abstract wrapper around the generated scalar column writers. Handles
 * overflow of vectors and retry of the write against a new batch. This
 * class is common to scalar columns and the members of arrays.
 */

public abstract class AbstractScalarLoader extends AbstractColumnLoader implements ScalarLoader {

  protected static final String ROLLOVER_FAILED = "Row batch rollover failed.";

  protected final WriterIndexImpl index;
  protected final ScalarWriter scalarWriter;

  protected AbstractScalarLoader(WriterIndexImpl index, ScalarWriter writer) {
    this.index = index;
    this.scalarWriter = writer;
  }

  @Override
  public ValueType valueType() { return scalarWriter.valueType(); }

  @Override
  public void setInt(int value) {
    assert index.legal();
    try {
      scalarWriter.setInt(value);
    } catch (VectorOverflowException e) {
      try {
        scalarWriter.setInt(value);
      } catch (VectorOverflowException e1) {
        throw new IllegalStateException(ROLLOVER_FAILED);
      }
    }
  }

  @Override
  public void setLong(long value) {
    assert index.legal();
    try {
      scalarWriter.setLong(value);
    } catch (VectorOverflowException e) {
      try {
        scalarWriter.setLong(value);
      } catch (VectorOverflowException e1) {
        throw new IllegalStateException(ROLLOVER_FAILED);
      }
    }
  }

  @Override
  public void setDouble(double value) {
    assert index.legal();
    try {
      scalarWriter.setDouble(value);
    } catch (VectorOverflowException e) {
      try {
        scalarWriter.setDouble(value);
      } catch (VectorOverflowException e1) {
        throw new IllegalStateException(ROLLOVER_FAILED);
      }
    }
  }

  @Override
  public void setString(String value) {
    assert index.legal();
    try {
      scalarWriter.setString(value);
    } catch (VectorOverflowException e) {
      try {
        scalarWriter.setString(value);
      } catch (VectorOverflowException e1) {
        throw new IllegalStateException(ROLLOVER_FAILED);
      }
    }
  }

  @Override
  public void setBytes(byte[] value) {
    setBytes(value, value.length);
  }

  @Override
  public void setBytes(byte[] value, int len) {
    assert index.legal();
    try {
      scalarWriter.setBytes(value, len);
    } catch (VectorOverflowException e) {
      try {
        scalarWriter.setBytes(value, len);
      } catch (VectorOverflowException e1) {
        throw new IllegalStateException(ROLLOVER_FAILED);
      }
    }
  }

  @Override
  public void setDecimal(BigDecimal value) {
    assert index.legal();
    try {
      scalarWriter.setDecimal(value);
    } catch (VectorOverflowException e) {
      try {
        scalarWriter.setDecimal(value);
      } catch (VectorOverflowException e1) {
        throw new IllegalStateException(ROLLOVER_FAILED);
      }
    }
  }

  @Override
  public void setPeriod(Period value) {
    assert index.legal();
    try {
      scalarWriter.setPeriod(value);
    } catch (VectorOverflowException e) {
      try {
        scalarWriter.setPeriod(value);
      } catch (VectorOverflowException e1) {
        throw new IllegalStateException(ROLLOVER_FAILED);
      }
    }
  }

  @Override
  public TupleLoader map() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrayLoader array() {
    throw new UnsupportedOperationException();
  }
}
