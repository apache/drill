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

/*
 * Here we compute the output scale and precision of the multiply function.
 * We simply add the input scale and precision to determine the output's scale
 * and precision
 */
public class DecimalScalePrecisionMulFunction extends DrillBaseComputeScalePrecision {

  public DecimalScalePrecisionMulFunction(int leftPrecision, int leftScale, int rightPrecision, int rightScale) {
    super(leftPrecision, leftScale, rightPrecision, rightScale);
  }

  @Override
  public void computeScalePrecision(int leftPrecision, int leftScale, int rightPrecision, int rightScale) {
    // compute the output scale and precision here
    outputScale = leftScale + rightScale;
    int integerDigits = (leftPrecision - leftScale) + (rightPrecision - rightScale);

    outputPrecision = integerDigits + outputScale;

    // If we are beyond the maximum precision range, cut down the fractional part
    if (outputPrecision > 38) {
      outputPrecision = 38;
      outputScale = (outputPrecision - integerDigits >= 0) ? (outputPrecision - integerDigits) : 0;
    }
  }
}

