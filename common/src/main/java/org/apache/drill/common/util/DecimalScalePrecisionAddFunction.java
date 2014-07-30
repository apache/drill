/**
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
package org.apache.drill.common.util;

public class DecimalScalePrecisionAddFunction extends DrillBaseComputeScalePrecision {

  public DecimalScalePrecisionAddFunction(int leftPrecision, int leftScale, int rightPrecision, int rightScale) {
    super(leftPrecision, leftScale, rightPrecision, rightScale);
  }

  @Override
  public void computeScalePrecision(int leftPrecision, int leftScale, int rightPrecision, int rightScale) {
    // compute the output scale and precision here
    outputScale = Math.max(leftScale, rightScale);
    int maxResultIntegerDigits = Math.max((leftPrecision - leftScale), (rightPrecision - rightScale)) + 1;

    outputPrecision = (outputScale + maxResultIntegerDigits);

    // If we are beyond the maximum precision range, cut down the fractional part
    if (outputPrecision > 38) {
      outputPrecision = 38;
      outputScale = (outputPrecision - maxResultIntegerDigits >= 0) ? (outputPrecision - maxResultIntegerDigits) : 0;
    }
  }
}
