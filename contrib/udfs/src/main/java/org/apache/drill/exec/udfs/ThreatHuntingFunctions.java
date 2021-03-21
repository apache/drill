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
package org.apache.drill.exec.udfs;

import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.DrillAggFunc;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.ComplexHolder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

public class ThreatHuntingFunctions {
  /**
   * Punctuation pattern is useful for comparing log entries.  It extracts the all the punctuation and returns
   * that pattern.  Spaces are replaced with an underscore.
   * <p>
   * Usage: SELECT punctuation_pattern( string ) FROM...
   */
  @FunctionTemplate(names = {"punctuation_pattern", "punctuationPattern"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class PunctuationPatternFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder rawInput;

    @Output
    VarCharHolder out;

    @Inject
    DrillBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {

      String input = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(rawInput.start, rawInput.end, rawInput.buffer);

      String punctuationPattern = input.replaceAll("[a-zA-Z0-9]", "");
      punctuationPattern = punctuationPattern.replaceAll(" ", "_");

      out.buffer = buffer;
      out.start = 0;
      out.end = punctuationPattern.getBytes().length;
      buffer.setBytes(0, punctuationPattern.getBytes());
    }
  }

  /**
   * This function calculates the Shannon Entropy of a given string of text.
   * See: https://en.wikipedia.org/wiki/Entropy_(information_theory) for full definition.
   * <p>
   * Usage:
   * SELECT entropy(<varchar>) AS entropy FROM...
   *
   * Returns a double
   */
  @FunctionTemplate(name = "entropy", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class StringEntropyFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder rawInput1;

    @Output
    Float8Holder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {

      String input = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(rawInput1.start, rawInput1.end, rawInput1.buffer);
      java.util.Set<Character> chars = new java.util.HashSet();

      for (char ch : input.toCharArray()) {
        chars.add(ch);
      }

      java.util.Map<Character, Double> probabilities = new java.util.HashMap();
      int length = input.length();

      // Get the probabilities
      for (Character character : chars) {
        double charCount = org.apache.commons.lang3.StringUtils.countMatches(input, character);
        double probability = charCount / length;
        probabilities.put(character, probability);
      }

      // Now get the entropy
      double entropy = 0.0;
      for (Double probability : probabilities.values()) {
        entropy += (probability * java.lang.Math.log(probability) / java.lang.Math.log(2.0));
      }
      out.value = Math.abs(entropy);
    }
  }

  /**
   * This function calculates the Shannon Entropy of a given string of text, normed for the string length.
   * See: https://en.wikipedia.org/wiki/Entropy_(information_theory) for full definition.
   * <p>
   * Usage:
   * SELECT entropy_per_byte(<varchar>) AS entropy FROM...
   *
   * Returns a double
   */
  @FunctionTemplate(names = {"entropy_per_byte", "entropyPerByte"},
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)

  public static class NormedStringEntropyFunction implements DrillSimpleFunc {

    @Param
    VarCharHolder rawInput;

    @Output
    Float8Holder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {

      String input =org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(rawInput);
      java.util.Set<Character> chars = new java.util.HashSet();

      for (char ch : input.toCharArray()) {
        chars.add(ch);
      }

      java.util.Map<Character, Double> probabilities = new java.util.HashMap();
      int length = input.length();

      // Get the probabilities
      for (Character character : chars) {
        double charCount = org.apache.commons.lang3.StringUtils.countMatches(input, character);
        double probability = charCount / length;
        probabilities.put(character, probability);
      }

      // Now get the entropy
      double entropy = 0.0;
      for (Double probability : probabilities.values()) {
        entropy += (probability * java.lang.Math.log(probability) / java.lang.Math.log(2.0));
      }

      if (input.length() == 0) {
        out.value = 0.0;
      } else {
        out.value = (Math.abs(entropy) / input.length());
      }
    }
  }

  /**
   * Examine the distribution of the first digits in a given corpus of numbers to see
   * if they correspond to Benford's Law using a chi square test.
   * Benford's Law, also known as the "first digit law" or the "law of anomalous numbers"
   * states that there is a specific distribution pattern of the first digits of certain
   * groups of numbers.  See https://en.wikipedia.org/wiki/Benford%27s_law for more
   * info.
   *
   * :param numbers: The set of numbers to check against Benford's Law
   * :type numbers: A list-like object (list, tuple, set, Pandas DataFrame or Series)
   *                   containing floats or integers
   * :Return Value:
   * The function returns three values in a tuple (chi2, p, counts):
   *  * The 'chi2' value is a float in the range 0..1 that describes how well the observed
   *    distribution of first digits matched the predictions of Benford's Law.  Lower is
   *    better.
   *  * The 'p' value is the probability that the computed 'chi2' is significant (i.e., it
   *    tells you whether the chi2 value can be trusted).  Its range is also 0..1, but in
   *    this case, higher is better.  Generally speaking, if the p-value is >= 0.95 then
   *    the chi2 value is considered significant.
   *  * 'counts' is a Pandas series where the indices are the possible first digits 1-9 and
   *     the values are the observed distributions of those digits. If the observed distributions
   *     didn't match up with Benford's law, the counts may help you identify the anomalous values.
   */


  /*
   def _first_digit(i: float):
        while i >= 10:
            i //= 10
        return trunc(i)

    _BENFORDS = [
        0.301,  # 1
        0.176,  # 2
        0.125,  # 3
        0.097,  # 4
        0.079,  # 5
        0.067,  # 6
        0.058,  # 7
        0.051,  # 8
        0.046  # 9
    ]

    if not is_list_like(numbers):
        raise TypeError(f'The argument must be a list or list-like of numbers, not type {type(numbers)}.')
    if isinstance(numbers, pd.core.series.Series):
        numbers = numbers.values

    numbers = pd.DataFrame(numbers, columns=['numbers'])
    numbers['digits'] = numbers['numbers'].apply(_first_digit)

    counts = numbers['digits'].value_counts()

    # No leading zeroes!
    if 0 in counts.index:
        counts = counts.drop(0)

    # Ensure every digit 1-9 has an count, even if it's 0
    for i in range(1, 10):
        if not i in counts:
            counts[i] = 0

    # Sort by index just to be extra sure they are all in the correct
    # order
    counts = counts.sort_index()

    # Compute the actual distribution of first digits in the input
    # as a proportion of that count to the entire number of samples
    num_samples = counts.sum()
    counts = counts.apply(lambda x: x/num_samples)

    # Compare the actual distribution to Benford's Law
    chi2, p = chisquare(counts.values, _BENFORDS)

    # Return the results of the comparison, plus the observed counts
    return chi2, p, counts

   */

  @FunctionTemplate(names = {"benfords", "benfordsDistribution"},
    scope = FunctionScope.POINT_AGGREGATE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class BenfordsFunction implements DrillAggFunc {

    @Param
    NullableIntHolder intHolder;

    @Output
    ComplexHolder out;

    @Override
    public void setup() {

    }

    @Override
    public void add() {

    }

    @Override
    public void output() {

    }

    @Override
    public void reset() {

    }
  }
}