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
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

import javax.inject.Inject;

public class ThreatHuntingFunctions {
  /**
   * Punctuation pattern is useful for comparing log entries.  It extracts all the punctuation and returns
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
      out.end = punctuationPattern.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
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
  @FunctionTemplate(name = "entropy",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
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

      String input = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(rawInput);
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
}
