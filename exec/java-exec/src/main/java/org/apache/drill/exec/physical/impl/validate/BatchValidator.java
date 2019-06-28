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
package org.apache.drill.exec.physical.impl.validate;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.record.SimpleVectorWrapper;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.vector.BaseDataValueVector;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.RepeatedVarCharVector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;
import org.apache.drill.exec.vector.complex.RepeatedFixedWidthVectorLike;


/**
 * Validate a batch of value vectors. It is not possible to validate the
 * data, but we can validate the structure, especially offset vectors.
 * Only handles single (non-hyper) vectors at present. Current form is
 * self-contained. Better checks can be done by moving checks inside
 * vectors or by exposing more metadata from vectors.
 */

public class BatchValidator {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BatchValidator.class);

  public static final int MAX_ERRORS = 100;

  private final int rowCount;
  private final VectorAccessible batch;
  private final List<String> errorList;
  private int errorCount;

  public BatchValidator(VectorAccessible batch) {
    rowCount = batch.getRecordCount();
    this.batch = batch;
    errorList = null;
  }

  public BatchValidator(VectorAccessible batch, boolean captureErrors) {
    rowCount = batch.getRecordCount();
    this.batch = batch;
    if (captureErrors) {
      errorList = new ArrayList<>();
    } else {
      errorList = null;
    }
  }

  public void validate() {
    if (batch.getRecordCount() == 0) {
      return;
    }
    for (VectorWrapper<? extends ValueVector> w : batch) {
      validateWrapper(w);
    }
  }

  private void validateWrapper(VectorWrapper<? extends ValueVector> w) {
    if (w instanceof SimpleVectorWrapper) {
      validateVector(w.getValueVector());
    }
  }

  private void validateVector(ValueVector vector) {
    String name = vector.getField().getName();
    if (vector instanceof NullableVector) {
      validateNullableVector(name, (NullableVector) vector);
    } else if (vector instanceof VariableWidthVector) {
      validateVariableWidthVector(name, (VariableWidthVector) vector, rowCount);
    } else if (vector instanceof FixedWidthVector) {
      validateFixedWidthVector(name, (FixedWidthVector) vector);
    } else if (vector instanceof BaseRepeatedValueVector) {
      validateRepeatedVector(name, (BaseRepeatedValueVector) vector);
    } else {
      logger.debug("Don't know how to validate vector: " + name + " of class " + vector.getClass().getSimpleName());
    }
  }

  private void validateVariableWidthVector(String name, VariableWidthVector vector, int entryCount) {

    // Offsets are in the derived classes. Handle only VarChar for now.

    if (vector instanceof VarCharVector) {
      validateVarCharVector(name, (VarCharVector) vector, entryCount);
    } else {
      logger.debug("Don't know how to validate vector: " + name + " of class " + vector.getClass().getSimpleName());
    }
  }

  private void validateVarCharVector(String name, VarCharVector vector, int entryCount) {
//    int dataLength = vector.getAllocatedByteCount(); // Includes offsets and data.
    int dataLength = vector.getBuffer().capacity();
    validateOffsetVector(name + "-offsets", vector.getOffsetVector(), entryCount, dataLength);
  }

  private void validateRepeatedVector(String name, BaseRepeatedValueVector vector) {

    int dataLength = Integer.MAX_VALUE;
    if (vector instanceof RepeatedVarCharVector) {
      dataLength = ((RepeatedVarCharVector) vector).getOffsetVector().getValueCapacity();
    } else if (vector instanceof RepeatedFixedWidthVectorLike) {
      dataLength = ((BaseDataValueVector) vector.getDataVector()).getBuffer().capacity();
    }
    int itemCount = validateOffsetVector(name + "-offsets", vector.getOffsetVector(), rowCount, dataLength);

    // Special handling of repeated VarChar vectors
    // The nested data vectors are not quite exactly like top-level vectors.

    ValueVector dataVector = vector.getDataVector();
    if (dataVector instanceof VariableWidthVector) {
      validateVariableWidthVector(name + "-data", (VariableWidthVector) dataVector, itemCount);
    }
  }

  private int validateOffsetVector(String name, UInt4Vector offsetVector, int valueCount, int maxOffset) {
    if (valueCount == 0) {
      return 0;
    }
    UInt4Vector.Accessor accessor = offsetVector.getAccessor();

    // First value must be zero in current version.

    int prevOffset = accessor.get(0);
    if (prevOffset != 0) {
      error(name, offsetVector, "Offset (0) must be 0 but was " + prevOffset);
    }

    // Note <= comparison: offset vectors have (n+1) entries.

    for (int i = 1; i <= valueCount; i++) {
      int offset = accessor.get(i);
      if (offset < prevOffset) {
        error(name, offsetVector, "Decreasing offsets at (" + (i-1) + ", " + i + ") = (" + prevOffset + ", " + offset + ")");
      } else if (offset > maxOffset) {
        error(name, offsetVector, "Invalid offset at index " + i + " = " + offset + " exceeds maximum of " + maxOffset);
      }
      prevOffset = offset;
    }
    return prevOffset;
  }

  private void error(String name, ValueVector vector, String msg) {
    if (errorCount == 0) {
      logger.error("Found one or more vector errors from " + batch.getClass().getSimpleName());
    }
    errorCount++;
    if (errorCount >= MAX_ERRORS) {
      return;
    }
    String fullMsg = "Column " + name + " of type " + vector.getClass().getSimpleName( ) + ": " + msg;
    logger.error(fullMsg);
    if (errorList != null) {
      errorList.add(fullMsg);
    }
  }

  private void validateNullableVector(String name, NullableVector vector) {
    // Can't validate at this time because the bits vector is in each
    // generated subtype.

    // Validate a VarChar vector because it is common.

    if (vector instanceof NullableVarCharVector) {
      VarCharVector values = ((NullableVarCharVector) vector).getValuesVector();
      validateVarCharVector(name + "-values", values, rowCount);
    }
  }

  private void validateFixedWidthVector(String name, FixedWidthVector vector) {
    // TODO Auto-generated method stub

  }

  /**
   * Obtain the list of errors. For use in unit-testing this class.
   * @return the list of errors found, or null if error capture was
   * not enabled
   */

  public List<String> errors() { return errorList; }
}
