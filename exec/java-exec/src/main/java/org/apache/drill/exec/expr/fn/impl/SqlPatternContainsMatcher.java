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

public final class SqlPatternContainsMatcher implements SqlPatternMatcher {
  final String patternString;
  CharSequence charSequenceWrapper;
  final int patternLength;

  public SqlPatternContainsMatcher(String patternString, CharSequence charSequenceWrapper) {
    this.patternString       = patternString;
    this.charSequenceWrapper = charSequenceWrapper;
    patternLength            = patternString.length();
  }

  @Override
  public final int match() {
    // The idea is to write loops with simple condition checks to allow the Java Hotspot vectorize
    // the generate code.
    if (patternLength == 1) {
      return match_1();
    } else if (patternLength == 2) {
      return match_2();
    } else if (patternLength == 3) {
      return match_3();
    } else {
      return match_N();
    }
  }

  private final int match_1() {
    final CharSequence sequenceWrapper = charSequenceWrapper;
    final int lengthToProcess          = sequenceWrapper.length();
    final char first_patt_char         = patternString.charAt(0);

    // simplePattern string has meta characters i.e % and _ and escape characters removed.
    // so, we can just directly compare.
    for (int idx = 0; idx < lengthToProcess; idx++) {
      char input_char = sequenceWrapper.charAt(idx);

      if (first_patt_char != input_char) {
        continue;
      }
      return 1;
    }
    return 0;
  }

  private final int match_2() {
    final CharSequence sequenceWrapper = charSequenceWrapper;
    final int lengthToProcess          = sequenceWrapper.length() - 1;
    final char first_patt_char         = patternString.charAt(0);

    // simplePattern string has meta characters i.e % and _ and escape characters removed.
    // so, we can just directly compare.
    for (int idx = 0; idx < lengthToProcess; idx++) {
      char input_char = sequenceWrapper.charAt(idx);

      if (first_patt_char != input_char) {
        continue;
      } else {
        char ch2_1 = sequenceWrapper.charAt(idx+1);
        char ch2_2 = patternString.charAt(1);

        if (ch2_1 == ch2_2) {
          return 1;
        }
      }
    }
    return 0;
  }

  private final int match_3() {
    final CharSequence sequenceWrapper = charSequenceWrapper;
    final int lengthToProcess          = sequenceWrapper.length() -2;
    final char first_patt_char         = patternString.charAt(0);

    // simplePattern string has meta characters i.e % and _ and escape characters removed.
    // so, we can just directly compare.
    for (int idx = 0; idx < lengthToProcess; idx++) {
      char input_char = sequenceWrapper.charAt(idx);

      if (first_patt_char != input_char) {
        continue;
      } else {
        char ch2_1 = sequenceWrapper.charAt(idx+1);
        char ch2_2 = patternString.charAt(1);
        char ch3_1 = sequenceWrapper.charAt(idx+2);
        char ch3_2 = patternString.charAt(2);

        if (ch2_1 == ch2_2 && ch3_1 == ch3_2) {
          return 1;
        }
      }
    }
    return 0;
  }

  private final int match_N() {

    if (patternLength == 0) {
      return 1;
    }

    final CharSequence sequenceWrapper = charSequenceWrapper;
    final int lengthToProcess          = sequenceWrapper.length() - patternLength +1;
    int patternIndex                   = 0;
    final char first_patt_char         = patternString.charAt(0);

    // simplePattern string has meta characters i.e % and _ and escape characters removed.
    // so, we can just directly compare.
    for (int idx = 0; idx < lengthToProcess; idx++) {
      char input_char = sequenceWrapper.charAt(idx);

      if (first_patt_char != input_char) {
        continue;
      } else {
        for (patternIndex = 1; patternIndex < patternLength; ++patternIndex) {
          char ch1 = sequenceWrapper.charAt(idx+patternIndex);
          char ch2 = patternString.charAt(patternIndex);

          if (ch1 != ch2) {
            break;
          }
        }

        if (patternIndex == patternLength) {
          break;
        }
      }
    }
    return patternIndex == patternLength ? 1 : 0;
  }

}
