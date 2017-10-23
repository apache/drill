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
  final MatcherFcn matcherFcn;

  public SqlPatternContainsMatcher(String patternString, CharSequence charSequenceWrapper) {
    this.patternString       = patternString;
    this.charSequenceWrapper = charSequenceWrapper;
    patternLength            = patternString.length();

    // The idea is to write loops with simple condition checks to allow the Java Hotspot achieve
    // better optimizations (especially vectorization)
    if (patternLength == 1) {
      matcherFcn = new Matcher1();
    } else if (patternLength == 2) {
      matcherFcn = new Matcher2();
    } else if (patternLength == 3) {
      matcherFcn = new Matcher3();
    } else {
      matcherFcn = new MatcherN();
    }
  }

  @Override
  public final int match() {
    return matcherFcn.match();
  }



  // --------------------------------------------------------------------------
  // Inner Data Structure
  // --------------------------------------------------------------------------

  /** Abstract matcher class to allow us pick the most efficient implementation */
  private abstract class MatcherFcn {
    /**
     * @return 1 if the pattern was matched; 0 otherwise
     */
    protected abstract int match();
  }

  /** Handles patterns with length one */
  private final class Matcher1 extends MatcherFcn {
    /** {@inheritDoc} */
    protected final int match() {
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
  }

  /** Handles patterns with length two */
  private final class Matcher2 extends MatcherFcn {
    /** {@inheritDoc} */
    protected final int match() {
      final CharSequence sequenceWrapper = charSequenceWrapper;
      final int lengthToProcess          = sequenceWrapper.length() - 1;
      final char first_patt_char         = patternString.charAt(0);
      final char second_patt_char        = patternString.charAt(1);

      // simplePattern string has meta characters i.e % and _ and escape characters removed.
      // so, we can just directly compare.
      for (int idx = 0; idx < lengthToProcess; idx++) {
        final char input_char = sequenceWrapper.charAt(idx);

        if (first_patt_char != input_char) {
          continue;
        } else {
          final char ch2 = sequenceWrapper.charAt(idx+1);

          if (ch2 == second_patt_char) {
            return 1;
          }
        }
      }
      return 0;
    }
  }

  /** Handles patterns with length three */
  private final class Matcher3 extends MatcherFcn {
    /** {@inheritDoc} */
    protected final int match() {
      final CharSequence sequenceWrapper = charSequenceWrapper;
      final int lengthToProcess          = sequenceWrapper.length() -2;
      final char first_patt_char         = patternString.charAt(0);
      final char second_patt_char        = patternString.charAt(1);
      final char third_patt_char         = patternString.charAt(2);

      // simplePattern string has meta characters i.e % and _ and escape characters removed.
      // so, we can just directly compare.
      for (int idx = 0; idx < lengthToProcess; idx++) {
        final char input_char = sequenceWrapper.charAt(idx);

        if (first_patt_char != input_char) {
          continue;
        } else {
          final char ch2 = sequenceWrapper.charAt(idx+1);
          final char ch3 = sequenceWrapper.charAt(idx+2);

          if (ch2 == second_patt_char && ch3 == third_patt_char) {
            return 1;
          }
        }
      }
      return 0;
    }
  }

  /** Handles patterns with arbitrary length */
  private final class MatcherN extends MatcherFcn {
    /** {@inheritDoc} */
    protected final int match() {

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
        final char input_char = sequenceWrapper.charAt(idx);

        if (first_patt_char != input_char) {
          continue;
        } else {
          for (patternIndex = 1; patternIndex < patternLength; ++patternIndex) {
            final char ch1 = sequenceWrapper.charAt(idx+patternIndex);
            final char ch2 = patternString.charAt(patternIndex);

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
}
