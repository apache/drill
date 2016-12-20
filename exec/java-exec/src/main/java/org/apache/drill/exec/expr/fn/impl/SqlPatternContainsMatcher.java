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
package org.apache.drill.exec.expr.fn.impl;

import io.netty.buffer.DrillBuf;

public class SqlPatternContainsMatcher extends AbstractSqlPatternMatcher {

  public SqlPatternContainsMatcher(String patternString) {
    super(patternString);
  }

  @Override
  public int match(int start, int end, DrillBuf drillBuf) {

    if (patternLength == 0) { // Everything should match for null pattern string
      return 1;
    }

    final int txtLength = end - start;

    // no match if input string length is less than pattern length
    if (txtLength < patternLength) {
      return 0;
    }


    final int outerEnd = txtLength - patternLength;

    outer:
    for (int txtIndex = 0; txtIndex <= outerEnd; txtIndex++) {

      // simplePattern string has meta characters i.e % and _ and escape characters removed.
      // so, we can just directly compare.
      for (int patternIndex = 0; patternIndex < patternLength; patternIndex++) {
        if (patternByteBuffer.get(patternIndex) != drillBuf.getByte(start + txtIndex + patternIndex)) {
          continue outer;
        }
      }

      return 1;
    }

    return  0;
  }

}
