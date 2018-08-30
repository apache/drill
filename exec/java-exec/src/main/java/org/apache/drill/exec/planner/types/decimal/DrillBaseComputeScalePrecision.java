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
package org.apache.drill.exec.planner.types.decimal;

import static org.apache.drill.exec.planner.types.DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM;

public abstract class DrillBaseComputeScalePrecision {
  protected final static int MAX_NUMERIC_PRECISION = DRILL_REL_DATATYPE_SYSTEM.getMaxNumericPrecision();
  protected final static int MAX_NUMERIC_SCALE = DRILL_REL_DATATYPE_SYSTEM.getMaxNumericScale();

  protected int outputScale = 0;
  protected int outputPrecision = 0;

  public int getOutputScale() {
    return outputScale;
  }

  public int getOutputPrecision() {
    return outputPrecision;
  }

  /**
   * Cuts down the fractional part if the current precision
   * exceeds the maximum precision range.
   */
  public void adjustPrecisionRange() {
    if (outputPrecision > MAX_NUMERIC_PRECISION) {
      outputScale = outputScale - (outputPrecision - MAX_NUMERIC_PRECISION);
      outputPrecision = MAX_NUMERIC_PRECISION;
    }
  }

  /**
   * Verifies if output scale and precision are valid or not
   */
  public void verifyScaleAndPrecision() {
    if (outputScale > outputPrecision) {
      throw new IllegalArgumentException(String.format("The output of this operation on decimal type is invalid as " +
          "computed scale is greater than precision. [Output expression precision: %d and scale: %d]",
        outputPrecision, outputScale));
    } else if (outputScale < 0 || outputPrecision > MAX_NUMERIC_PRECISION || outputScale > MAX_NUMERIC_SCALE) {
      throw new IllegalArgumentException(String.format("The output of this operation on decimal type is out of range." +
        " Drill supports max precision of %d and non-negative maximum scale %d. [Output expression precision: %d and " +
          "scale: %d]", MAX_NUMERIC_PRECISION, MAX_NUMERIC_SCALE, outputPrecision, outputScale));
    }
  }
}
