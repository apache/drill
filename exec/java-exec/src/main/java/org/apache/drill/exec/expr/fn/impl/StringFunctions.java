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


import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;

import java.nio.charset.Charset;

import javax.inject.Inject;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;

public class StringFunctions{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StringFunctions.class);

  private StringFunctions() {}

  /*
   * String Function Implementation.
   */

  @FunctionTemplate(name = "like", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Like implements DrillSimpleFunc {

    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Output BitHolder out;
    @Workspace java.util.regex.Matcher matcher;
    @Workspace org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper charSequenceWrapper;

    @Override
    public void setup() {
      matcher = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.RegexpUtil.sqlToRegexLike( //
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(pattern.start,  pattern.end,  pattern.buffer))).matcher("");
      charSequenceWrapper = new org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper();
      matcher.reset(charSequenceWrapper);
    }

    @Override
    public void eval() {
      charSequenceWrapper.setBuffer(input.start, input.end, input.buffer);
      // Reusing same charSequenceWrapper, no need to pass it in.
      // This saves one method call since reset(CharSequence) calls reset()
      matcher.reset();
      out.value = matcher.matches()? 1:0;
    }
  }

  @FunctionTemplate(name = "like", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class LikeWithEscape implements DrillSimpleFunc {

    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Param(constant=true) VarCharHolder escape;
    @Output BitHolder out;
    @Workspace java.util.regex.Matcher matcher;
    @Workspace org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper charSequenceWrapper;

    @Override
    public void setup() {
      matcher = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.RegexpUtil.sqlToRegexLike( //
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(pattern.start,  pattern.end,  pattern.buffer),
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(escape.start,  escape.end,  escape.buffer))).matcher("");
      charSequenceWrapper = new org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper();
      matcher.reset(charSequenceWrapper);
    }

    @Override
    public void eval() {
      charSequenceWrapper.setBuffer(input.start, input.end, input.buffer);
      // Reusing same charSequenceWrapper, no need to pass it in.
      // This saves one method call since reset(CharSequence) calls reset()
      matcher.reset();
      out.value = matcher.matches()? 1:0;
    }
  }

  @FunctionTemplate(name = "ilike", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ILike implements DrillSimpleFunc {

    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Output BitHolder out;
    @Workspace java.util.regex.Matcher matcher;
    @Workspace org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper charSequenceWrapper;

    @Override
    public void setup() {
      matcher = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.RegexpUtil.sqlToRegexLike( //
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(pattern.start,  pattern.end,  pattern.buffer)),
          java.util.regex.Pattern.CASE_INSENSITIVE).matcher("");
      charSequenceWrapper = new org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper();
      matcher.reset(charSequenceWrapper);
    }

    @Override
    public void eval() {
      charSequenceWrapper.setBuffer(input.start, input.end, input.buffer);
      // Reusing same charSequenceWrapper, no need to pass it in.
      // This saves one method call since reset(CharSequence) calls reset()
      matcher.reset();
      out.value = matcher.matches()? 1:0;
    }
  }

  @FunctionTemplate(name = "ilike", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ILikeWithEscape implements DrillSimpleFunc {

    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Param(constant=true) VarCharHolder escape;
    @Output BitHolder out;
    @Workspace java.util.regex.Matcher matcher;
    @Workspace org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper charSequenceWrapper;

    @Override
    public void setup() {
      matcher = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.RegexpUtil.sqlToRegexLike( //
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(pattern.start,  pattern.end,  pattern.buffer),
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(escape.start,  escape.end,  escape.buffer)),
          java.util.regex.Pattern.CASE_INSENSITIVE).matcher("");
      charSequenceWrapper = new org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper();
      matcher.reset(charSequenceWrapper);
    }

    @Override
    public void eval() {
      charSequenceWrapper.setBuffer(input.start, input.end, input.buffer);
      // Reusing same charSequenceWrapper, no need to pass it in.
      // This saves one method call since reset(CharSequence) calls reset()
      matcher.reset();
      out.value = matcher.matches()? 1:0;
    }
  }

  @FunctionTemplate(names = {"similar", "similar_to"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Similar implements DrillSimpleFunc {
    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Output BitHolder out;
    @Workspace java.util.regex.Matcher matcher;
    @Workspace org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper charSequenceWrapper;

    @Override
    public void setup() {
      matcher = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.RegexpUtil.sqlToRegexSimilar(
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers
              .toStringFromUTF8(pattern.start, pattern.end, pattern.buffer))).matcher("");
      charSequenceWrapper = new org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper();
      matcher.reset(charSequenceWrapper);
    }

    @Override
    public void eval() {
      charSequenceWrapper.setBuffer(input.start, input.end, input.buffer);
      // Reusing same charSequenceWrapper, no need to pass it in.
      // This saves one method call since reset(CharSequence) calls reset()
      matcher.reset();
      out.value = matcher.matches()? 1:0;
    }
  }

  @FunctionTemplate(names = {"similar", "similar_to"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class SimilarWithEscape implements DrillSimpleFunc {
    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Param(constant=true) VarCharHolder escape;
    @Output BitHolder out;
    @Workspace java.util.regex.Matcher matcher;
    @Workspace org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper charSequenceWrapper;

    @Override
    public void setup() {
      matcher = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.RegexpUtil.sqlToRegexSimilar(
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(pattern.start,  pattern.end,  pattern.buffer),
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(escape.start,  escape.end,  escape.buffer))).matcher("");
      charSequenceWrapper = new org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper();
      matcher.reset(charSequenceWrapper);
    }

    @Override
    public void eval() {
      charSequenceWrapper.setBuffer(input.start, input.end, input.buffer);
      // Reusing same charSequenceWrapper, no need to pass it in.
      // This saves one method call since reset(CharSequence) calls reset()
      matcher.reset();
      out.value = matcher.matches()? 1:0;
    }
  }

  /*
   * Replace all substring that match the regular expression with replacement.
   */
  @FunctionTemplate(name = "regexp_replace", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RegexpReplace implements DrillSimpleFunc {

    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Param VarCharHolder replacement;
    @Inject DrillBuf buffer;
    @Workspace java.util.regex.Matcher matcher;
    @Workspace org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper charSequenceWrapper;
    @Output VarCharHolder out;

    @Override
    public void setup() {
      matcher = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(
          pattern.start, pattern.end, pattern.buffer)).matcher("");
      charSequenceWrapper = new org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper();
      matcher.reset(charSequenceWrapper);
    }

    @Override
    public void eval() {
      out.start = 0;
      charSequenceWrapper.setBuffer(input.start, input.end, input.buffer);
      final String r = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(replacement.start, replacement.end, replacement.buffer);
      // Reusing same charSequenceWrapper, no need to pass it in.
      matcher.reset();
      // Implementation of Matcher.replaceAll() in-lined to avoid creating String object
      // in cases where we don't actually replace anything.
      boolean result = matcher.find();
      if (result) {
          StringBuffer sb = new StringBuffer();
          do {
              matcher.appendReplacement(sb, r);
              result = matcher.find();
          } while (result);
          matcher.appendTail(sb);
          final byte [] bytea = sb.toString().getBytes(java.nio.charset.Charset.forName("UTF-8"));
          out.buffer = buffer = buffer.reallocIfNeeded(bytea.length);
          out.buffer.setBytes(out.start, bytea);
          out.end = bytea.length;
      }
      else {
          // There is no matches, copy the input bytes into the output buffer
          out.buffer = buffer = buffer.reallocIfNeeded(input.end - input.start);
          out.buffer.setBytes(0, input.buffer, input.start, input.end - input.start);
          out.end = input.end - input.start;
      }
    }
  }

  /*
   * Match the given input against a regular expression.
   *
   * This differs from the "similar" function in that accepts a standard regex, rather than a SQL regex.
   */
  @FunctionTemplate(name = "regexp_matches", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RegexpMatches implements DrillSimpleFunc {

    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Inject DrillBuf buffer;
    @Workspace java.util.regex.Matcher matcher;
    @Workspace org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper charSequenceWrapper;
    @Output BitHolder out;

    @Override
    public void setup() {
      matcher = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(pattern.start,  pattern.end,  pattern.buffer)).matcher("");
      charSequenceWrapper = new org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper();
      matcher.reset(charSequenceWrapper);
    }

    @Override
    public void eval() {
      charSequenceWrapper.setBuffer(input.start, input.end, input.buffer);
      // Reusing same charSequenceWrapper, no need to pass it in.
      // This saves one method call since reset(CharSequence) calls reset()
      matcher.reset();
      out.value = matcher.matches()? 1:0;
    }
  }

  @FunctionTemplate(names = {"char_length", "character_length", "length"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class CharLength implements DrillSimpleFunc {
    @Param  VarCharHolder input;
    @Output BigIntHolder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(input.buffer, input.start, input.end);
    }
  }

  @FunctionTemplate(name = "lengthUtf8", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ByteLength implements DrillSimpleFunc {
    @Param  VarBinaryHolder input;
    @Output BigIntHolder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(input.buffer, input.start, input.end);
    }
  }

  @FunctionTemplate(name = "octet_length", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class OctetLength implements DrillSimpleFunc {
    @Param  VarCharHolder input;
    @Output BigIntHolder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      out.value = input.end - input.start;
    }
  }

  @FunctionTemplate(name = "bit_length", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class BitLength implements DrillSimpleFunc {
    @Param  VarCharHolder input;
    @Output BigIntHolder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      out.value = (input.end - input.start) * 8;
    }
  }

  /*
   * Location of specified substring.
   *
   * Difference from PostgreSQL :
   *          exp \ System                PostgreSQL            Drill
   *     position('', 'abc')                1                     0
   *     position('', '')                   1                     0
   */
  @FunctionTemplate(name = "position", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Position implements DrillSimpleFunc {
    @Param  VarCharHolder substr;
    @Param  VarCharHolder str;

    @Output BigIntHolder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      //Do string match.
      final int pos = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.stringLeftMatchUTF8(str.buffer, str.start, str.end,
                                                                                          substr.buffer, substr.start, substr.end);
      if (pos < 0) {
        out.value = 0; //indicate not found a matched substr.
      } else {
        //Count the # of characters. (one char could have 1-4 bytes)
        out.value = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(str.buffer, str.start, pos) + 1;
      }
    }

  }


  @FunctionTemplate(name = "split_part", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class SplitPart implements DrillSimpleFunc {
    @Param  VarCharHolder str;
    @Param  VarCharHolder splitter;
    @Param  IntHolder index;

    @Output VarCharHolder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      if (index.value < 1) {
        throw org.apache.drill.common.exceptions.UserException.functionError()
            .message("Index in split_part must be positive, value provided was " + index.value).build();
      }
      int bufPos = str.start;
      out.start = bufPos;
      boolean beyondLastIndex = false;
      int splitterLen = (splitter.end - splitter.start);
      for (int i = 1; i < index.value + 1; i++) {
        //Do string match.
        final int pos = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.stringLeftMatchUTF8(str.buffer,
            bufPos, str.end,
            splitter.buffer, splitter.start, splitter.end);
        if (pos < 0) {
          // this is the last iteration, it is okay to hit the end of the string
          if (i == index.value) {
            bufPos = str.end;
            // when the output is terminated by the end of the string we do not want
            // to subtract the length of the splitter from the output at the end of
            // the function below
            splitterLen = 0;
            break;
          } else {
            beyondLastIndex = true;
            break;
          }
        } else {
          // Count the # of characters. (one char could have 1-4 bytes)
          // unlike the position function don't add 1, we are not translating the positions into SQL user level 1 based indices
          bufPos = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(str.buffer, str.start, pos)
              + splitterLen;
          // if this is the second to last iteration, store the position again, as the start and end of the
          // string to be returned need to be available
          if (i == index.value - 1) {
            out.start = bufPos;
          }
        }
      }
      if (beyondLastIndex) {
        out.start = 0;
        out.end = 0;
        out.buffer = str.buffer;
      } else {
        out.buffer = str.buffer;
        out.end = bufPos - splitterLen;
      }
    }

  }

  // same as function "position(substr, str) ", except the reverse order of argument.
  @FunctionTemplate(name = "strpos", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Strpos implements DrillSimpleFunc {
    @Param  VarCharHolder str;
    @Param  VarCharHolder substr;

    @Output BigIntHolder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      //Do string match.
      int pos = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.stringLeftMatchUTF8(str.buffer, str.start, str.end,
                                                                                          substr.buffer, substr.start, substr.end);
      if (pos < 0) {
        out.value = 0; //indicate not found a matched substr.
      } else {
        //Count the # of characters. (one char could have 1-4 bytes)
        out.value = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(str.buffer, str.start, pos) + 1;
      }
    }
  }

  /*
   * Convert string to lower case.
   */
  @FunctionTemplate(name = "lower", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class LowerCase implements DrillSimpleFunc {
    @Param VarCharHolder input;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = buffer = buffer.reallocIfNeeded(input.end- input.start);
      out.start = 0;
      out.end = input.end - input.start;

      for (int id = input.start; id < input.end; id++) {
        byte  currentByte = input.buffer.getByte(id);

        // 'A - Z' : 0x41 - 0x5A
        // 'a - z' : 0x61 - 0x7A
        if (currentByte >= 0x41 && currentByte <= 0x5A) {
          currentByte += 0x20;
        }
        out.buffer.setByte(id - input.start, currentByte) ;
      }
    }
  }

  /*
   * Convert string to upper case.
   */
  @FunctionTemplate(name = "upper", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class UpperCase implements DrillSimpleFunc {

    @Param VarCharHolder input;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = buffer = buffer.reallocIfNeeded(input.end- input.start);
      out.start = 0;
      out.end = input.end - input.start;

      for (int id = input.start; id < input.end; id++) {
        byte currentByte = input.buffer.getByte(id);

        // 'A - Z' : 0x41 - 0x5A
        // 'a - z' : 0x61 - 0x7A
        if (currentByte >= 0x61 && currentByte <= 0x7A) {
          currentByte -= 0x20;
        }
        out.buffer.setByte(id - input.start, currentByte) ;
      }
    }
  }


  // Follow Postgre.
  //  -- Valid "offset": [1, string_length],
  //  -- Valid "length": [1, up to string_length - offset + 1], if length > string_length - offset +1, get the substr up to the string_lengt.
  @FunctionTemplate(names = {"substring", "substr"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Substring implements DrillSimpleFunc {
    @Param VarCharHolder string;
    @Param BigIntHolder offset;
    @Param BigIntHolder length;

    @Output VarCharHolder out;
    @Workspace ByteBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = string.buffer;
      // if length is NOT positive, or offset is NOT positive, or input string is empty, return empty string.
      if (length.value <= 0 || offset.value <=0 || string.end <= string.start) {
        out.start = out.end = 0;
      } else {
        //Do 1st scan to counter # of character in string.
        final int charCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(string.buffer, string.start, string.end);
        final int fromCharIdx = (int) offset.value; //the start position of char  (inclusive)

        if (fromCharIdx > charCount ) { // invalid length, return empty string.
          out.start = out.end = 0;
        } else {
          out.start = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(string.buffer, string.start, string.end, fromCharIdx-1);

          // Bounded length by charCount - fromCharIdx + 1. substring("abc", 1, 5) --> "abc"
          int charLen = Math.min((int)length.value, charCount - fromCharIdx + 1);

          out.end = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(string.buffer, out.start, string.end, charLen);
        }
      }
    }
  }

  @FunctionTemplate(names = {"substring", "substr"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class SubstringOffset implements DrillSimpleFunc {
    @Param VarCharHolder string;
    @Param BigIntHolder offset;

    @Output VarCharHolder out;
    @Workspace ByteBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = string.buffer;
      // if length is NOT positive, or offset is NOT positive, or input string is empty, return empty string.
      if (offset.value <=0 || string.end <= string.start) {
        out.start = out.end = 0;
      } else {
        //Do 1st scan to counter # of character in string.
        final int charCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(string.buffer, string.start, string.end);
        final int fromCharIdx = (int) offset.value; //the start position of char  (inclusive)

        if (fromCharIdx > charCount ) { // invalid length, return empty string.
          out.start = out.end = 0;
        } else {
          out.start = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(string.buffer, string.start, string.end, fromCharIdx-1);
          out.end = string.end;
        }
      }
    }
  }

  @FunctionTemplate(names = {"substring", "substr" }, scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class SubstringRegex implements DrillSimpleFunc {
    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Output NullableVarCharHolder out;
    @Workspace java.util.regex.Matcher matcher;
    @Workspace org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper charSequenceWrapper;

    @Override
    public void setup() {
      matcher = java.util.regex.Pattern.compile(
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(pattern.start,  pattern.end,  pattern.buffer))
          .matcher("");
      charSequenceWrapper = new org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper();
      matcher.reset(charSequenceWrapper);
    }

    @Override
    public void eval() {
      charSequenceWrapper.setBuffer(input.start, input.end, input.buffer);
      // Reusing same charSequenceWrapper, no need to pass it in.
      // This saves one method call since reset(CharSequence) calls reset()
      matcher.reset();
      if (matcher.find()) {
        out.isSet = 1;
        out.buffer = input.buffer;
        out.start = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(input.buffer, input.start, input.end, matcher.start());
        out.end = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(input.buffer, input.start, input.end, matcher.end());
      }
    }
  }

  @FunctionTemplate(names = {"substring", "substr" }, scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class SubstringRegexNullable implements DrillSimpleFunc {
    @Param NullableVarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Output NullableVarCharHolder out;
    @Workspace java.util.regex.Matcher matcher;
    @Workspace org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper charSequenceWrapper;

    @Override
    public void setup() {
      matcher = java.util.regex.Pattern.compile(
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(pattern.start,  pattern.end,  pattern.buffer))
          .matcher("");
      charSequenceWrapper = new org.apache.drill.exec.expr.fn.impl.CharSequenceWrapper();
      matcher.reset(charSequenceWrapper);
    }

    @Override
    public void eval() {
      if (input.isSet == 0) {
        out.isSet = 0;
      } else {
        charSequenceWrapper.setBuffer(input.start, input.end, input.buffer);
        // Reusing same charSequenceWrapper, no need to pass it in.
        // This saves one method call since reset(CharSequence) calls reset()
        matcher.reset();
        if (matcher.find()) {
          out.isSet = 1;
          out.buffer = input.buffer;
          out.start = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(input.buffer, input.start, input.end, matcher.start());
          out.end = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(input.buffer, input.start, input.end, matcher.end());
        } else {
          out.isSet = 0;
        }
      }
    }
  }

  // Return first length characters in the string. When length is negative, return all but last |length| characters.
  // If length > total charcounts, return the whole string.
  // If length = 0, return empty
  // If length < 0, and |length| > total charcounts, return empty.
  @FunctionTemplate(name = "left", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Left implements DrillSimpleFunc {
    @Param VarCharHolder string;
    @Param BigIntHolder length;

    @Output VarCharHolder out;
    @Workspace ByteBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = string.buffer;
      // if length is 0, or input string is empty, return empty string.
      if (length.value == 0 || string.end <= string.start) {
        out.start = out.end = 0;
      } else {
        //Do 1st scan to counter # of character in string.
        final int charCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(string.buffer, string.start, string.end);
        final int charLen;
        if (length.value > 0) {
          charLen = Math.min((int)length.value, charCount);  //left('abc', 5) -> 'abc'
        } else if (length.value < 0) {
          charLen = Math.max(0, charCount + (int)length.value) ; // left('abc', -5) ==> ''
        } else {
          charLen = 0;
        }

        out.start = string.start; //Starting from the left of input string.
        out.end = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(string.buffer, out.start, string.end, charLen);
      } // end of lenth.value != 0
    }
  }

  //Return last 'length' characters in the string. When 'length' is negative, return all but first |length| characters.
  @FunctionTemplate(name = "right", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Right implements DrillSimpleFunc {
    @Param VarCharHolder string;
    @Param BigIntHolder length;

    @Output VarCharHolder out;
    @Workspace ByteBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = string.buffer;
      // invalid length.
      if (length.value == 0 || string.end <= string.start) {
        out.start = out.end = 0;
      } else {
        //Do 1st scan to counter # of character in string.
        final int charCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(string.buffer, string.start, string.end);
        final int fromCharIdx; //the start position of char (inclusive)
        final int charLen; // the end position of char (inclusive)
        if (length.value > 0) {
          fromCharIdx = Math.max(charCount - (int) length.value + 1, 1); // right('abc', 5) ==> 'abc' fromCharIdx=1.
          charLen = charCount - fromCharIdx + 1;
        } else { // length.value < 0
          fromCharIdx = Math.abs((int) length.value) + 1;
          charLen = charCount - fromCharIdx +1;
        }

        // invalid length :  right('abc', -5) -> ''
        if (charLen <= 0) {
          out.start = out.end = 0;
        } else {
          //Do 2nd scan of string. Get bytes corresponding chars in range.
          out.start = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(string.buffer, string.start, string.end, fromCharIdx-1);
          out.end = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(string.buffer, out.start, string.end, charLen);
        }
      }
    }
  }


  @FunctionTemplate(name = "initcap", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class InitCap implements DrillSimpleFunc {
    @Param VarCharHolder input;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = buffer = buffer.reallocIfNeeded(input.end - input.start);
      out.start = 0;
      out.end = input.end - input.start;
      org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.initCap(input.start, input.end, input.buffer, out.buffer);
    }

  }

  //Replace all occurrences in 'text' of substring 'from' with substring 'to'
  @FunctionTemplate(name = "replace", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Replace implements DrillSimpleFunc {
    @Param  VarCharHolder text;
    @Param  VarCharHolder from;
    @Param  VarCharHolder to;
    @Inject DrillBuf buffer;
    @Output VarCharHolder out;

    @Override
    public void setup() {
      buffer = buffer.reallocIfNeeded(8000);
    }

    @Override
    public void eval() {
      out.buffer = buffer;
      out.start = out.end = 0;
      int fromL = from.end - from.start;
      int textL = text.end - text.start;

      if (fromL > 0 && fromL <= textL) {
        //If "from" is not empty and it's length is no longer than text's length
        //then, we may find a match, and do replace.
        int i = text.start;
        for (; i <= text.end - fromL; ) {
          int j = from.start;
          for (; j < from.end; j++) {
            if (text.buffer.getByte(i + j - from.start) != from.buffer.getByte(j)) {
              break;
            }
          }

          if (j == from.end ) {
            //find a true match ("from" is not empty), copy entire "to" string to out buffer
            for (int k = to.start ; k < to.end; k++) {
              out.buffer.setByte(out.end++, to.buffer.getByte(k));
            }

            //advance i by the length of "from"
            i += from.end - from.start;
          } else {
            //no match. copy byte i in text, advance i by 1.
            out.buffer.setByte(out.end++, text.buffer.getByte(i++));
          }
        }

        //Copy the tail part of text (length < fromL).
        for (; i < text.end; i++) {
          out.buffer.setByte(out.end++, text.buffer.getByte(i));
        }
      } else {
        //If "from" is empty or its length is larger than text's length,
        //then, we just set "out" as "text".
        out.buffer = text.buffer;
        out.start = text.start;
        out.end = text.end;
      }
    } // end of eval()
  }

  /*
   * Fill up the string to length 'length' by prepending the characters 'fill' in the beginning of 'text'.
   * If the string is already longer than length, then it is truncated (on the right).
   */
  @FunctionTemplate(name = "lpad", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Lpad implements DrillSimpleFunc {
    @Param  VarCharHolder text;
    @Param  BigIntHolder length;
    @Param  VarCharHolder fill;
    @Inject DrillBuf buffer;

    @Output VarCharHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      final long theLength = length.value;
      final int lengthNeeded = (int) (theLength <= 0 ? 0 : theLength * 2);
      buffer = buffer.reallocIfNeeded(lengthNeeded);
      byte currentByte = 0;
      int id = 0;
      //get the char length of text.
      int textCharCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(text.buffer, text.start, text.end);

      //get the char length of fill.
      int fillCharCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(fill.buffer, fill.start, fill.end);

      if (theLength <= 0) {
        //case 1: target length is <=0, then return an empty string.
        out.buffer = buffer;
        out.start = out.end = 0;
      } else if (theLength == textCharCount || (theLength > textCharCount  && fillCharCount == 0) ) {
        //case 2: target length is same as text's length, or need fill into text but "fill" is empty, then return text directly.
        out.buffer = text.buffer;
        out.start = text.start;
        out.end = text.end;
      } else if (theLength < textCharCount) {
        //case 3: truncate text on the right side. It's same as substring(text, 1, length).
        out.buffer = text.buffer;
        out.start = text.start;
        out.end = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(text.buffer, text.start, text.end, (int) theLength);
      } else if (theLength > textCharCount) {
        //case 4: copy "fill" on left. Total # of char to copy : theLength - textCharCount
        int count = 0;
        out.buffer = buffer;
        out.start = out.end = 0;

        while (count < theLength - textCharCount) {
          for (id = fill.start; id < fill.end; id++) {
            if (count == theLength - textCharCount) {
              break;
            }

            currentByte = fill.buffer.getByte(id);
            if (currentByte < 0x128  ||           // 1-byte char. First byte is 0xxxxxxx.
                (currentByte & 0xE0) == 0xC0 ||   // 2-byte char. First byte is 110xxxxx
                (currentByte & 0xF0) == 0xE0 ||   // 3-byte char. First byte is 1110xxxx
                (currentByte & 0xF8) == 0xF0) {   //4-byte char. First byte is 11110xxx
              count ++;  //Advance the counter, since we find one char.
            }
            out.buffer.setByte(out.end++, currentByte);
          }
        } // end of while

        //copy "text" into "out"
        for (id = text.start; id < text.end; id++) {
          out.buffer.setByte(out.end++, text.buffer.getByte(id));
        }
      }
    } // end of eval
  }

  /*
   * Fill up the string to length 'length' by prepending the character ' ' in the beginning of 'text'.
   * If the string is already longer than length, then it is truncated (on the right).
   */
  @FunctionTemplate(name = "lpad", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class LpadTwoArg implements DrillSimpleFunc {
    @Param  VarCharHolder text;
    @Param  BigIntHolder length;
    @Inject DrillBuf buffer;

    @Output VarCharHolder out;
    @Workspace byte spaceInByte;

    @Override
    public void setup() {
      spaceInByte = 32;
    }

    @Override
    public void eval() {
      final long theLength = length.value;
      final int lengthNeeded = (int) (theLength <= 0 ? 0 : theLength * 2);
      buffer = buffer.reallocIfNeeded(lengthNeeded);
      //get the char length of text.
      int textCharCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(text.buffer, text.start, text.end);

      if (theLength <= 0) {
        //case 1: target length is <=0, then return an empty string.
        out.buffer = buffer;
        out.start = out.end = 0;
      } else if (theLength == textCharCount) {
        //case 2: target length is same as text's length.
        out.buffer = text.buffer;
        out.start = text.start;
        out.end = text.end;
      } else if (theLength < textCharCount) {
        //case 3: truncate text on the right side. It's same as substring(text, 1, length).
        out.buffer = text.buffer;
        out.start = text.start;
        out.end = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(text.buffer, text.start, text.end, (int) theLength);
      } else if (theLength > textCharCount) {
        //case 4: copy " " on left. Total # of char to copy : theLength - textCharCount
        int count = 0;
        out.buffer = buffer;
        out.start = out.end = 0;

        while (count < theLength - textCharCount) {
          out.buffer.setByte(out.end++, spaceInByte);
          ++count;
        } // end of while

        //copy "text" into "out"
        for (int id = text.start; id < text.end; id++) {
          out.buffer.setByte(out.end++, text.buffer.getByte(id));
        }
      }
    } // end of eval
  }

  /**
   * Fill up the string to length "length" by appending the characters 'fill' at the end of 'text'
   * If the string is already longer than length then it is truncated.
   */
  @FunctionTemplate(name = "rpad", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Rpad implements DrillSimpleFunc {
    @Param  VarCharHolder text;
    @Param  BigIntHolder length;
    @Param  VarCharHolder fill;
    @Inject DrillBuf buffer;

    @Output VarCharHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      final long theLength = length.value;
      final int lengthNeeded = (int) (theLength <= 0 ? 0 : theLength * 2);
      buffer = buffer.reallocIfNeeded(lengthNeeded);

      byte currentByte = 0;
      int id = 0;
      //get the char length of text.
      int textCharCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(text.buffer, text.start, text.end);

      //get the char length of fill.
      int fillCharCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(fill.buffer, fill.start, fill.end);

      if (theLength <= 0) {
        //case 1: target length is <=0, then return an empty string.
        out.buffer = buffer;
        out.start = out.end = 0;
      } else if (theLength == textCharCount || (theLength > textCharCount  && fillCharCount == 0) ) {
        //case 2: target length is same as text's length, or need fill into text but "fill" is empty, then return text directly.
        out.buffer = text.buffer;
        out.start = text.start;
        out.end = text.end;
      } else if (theLength < textCharCount) {
        //case 3: truncate text on the right side. It's same as substring(text, 1, length).
        out.buffer = text.buffer;
        out.start = text.start;
        out.end = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(text.buffer, text.start, text.end, (int) theLength);
      } else if (theLength > textCharCount) {
        //case 4: copy "text" into "out", then copy "fill" on the right.
        out.buffer = buffer;
        out.start = out.end = 0;

        for (id = text.start; id < text.end; id++) {
          out.buffer.setByte(out.end++, text.buffer.getByte(id));
        }

        //copy "fill" on right. Total # of char to copy : theLength - textCharCount
        int count = 0;

        while (count < theLength - textCharCount) {
          for (id = fill.start; id < fill.end; id++) {
            if (count == theLength - textCharCount) {
              break;
            }

            currentByte = fill.buffer.getByte(id);
            if (currentByte < 0x128  ||           // 1-byte char. First byte is 0xxxxxxx.
                (currentByte & 0xE0) == 0xC0 ||   // 2-byte char. First byte is 110xxxxx
                (currentByte & 0xF0) == 0xE0 ||   // 3-byte char. First byte is 1110xxxx
                (currentByte & 0xF8) == 0xF0) {   //4-byte char. First byte is 11110xxx
              count ++;  //Advance the counter, since we find one char.
            }
            out.buffer.setByte(out.end++, currentByte);
          }
        } // end of while

      }
    } // end of eval
  }

  /**
   * Fill up the string to length "length" by appending the characters ' ' at the end of 'text'
   * If the string is already longer than length then it is truncated.
   */
  @FunctionTemplate(name = "rpad", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RpadTwoArg implements DrillSimpleFunc {
    @Param  VarCharHolder text;
    @Param  BigIntHolder length;
    @Inject DrillBuf buffer;

    @Output VarCharHolder out;
    @Workspace byte spaceInByte;

    @Override
    public void setup() {
      spaceInByte = 32;
    }

    @Override
    public void eval() {
      final long theLength = length.value;
      final int lengthNeeded = (int) (theLength <= 0 ? 0 : theLength * 2);
      buffer = buffer.reallocIfNeeded(lengthNeeded);

      //get the char length of text.
      int textCharCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(text.buffer, text.start, text.end);

      if (theLength <= 0) {
        //case 1: target length is <=0, then return an empty string.
        out.buffer = buffer;
        out.start = out.end = 0;
      } else if (theLength == textCharCount) {
        //case 2: target length is same as text's length.
        out.buffer = text.buffer;
        out.start = text.start;
        out.end = text.end;
      } else if (theLength < textCharCount) {
        //case 3: truncate text on the right side. It's same as substring(text, 1, length).
        out.buffer = text.buffer;
        out.start = text.start;
        out.end = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(text.buffer, text.start, text.end, (int) theLength);
      } else if (theLength > textCharCount) {
        //case 4: copy "text" into "out", then copy " " on the right.
        out.buffer = buffer;
        out.start = out.end = 0;

        for (int id = text.start; id < text.end; id++) {
          out.buffer.setByte(out.end++, text.buffer.getByte(id));
        }

        //copy " " on right. Total # of char to copy : theLength - textCharCount
        int count = 0;

        while (count < theLength - textCharCount) {
          out.buffer.setByte(out.end++, spaceInByte);
          ++count;
        } // end of while

      }
    } // end of eval
  }

  /**
   * Remove the longest string containing only characters from "from"  from the start of "text"
   */
  @FunctionTemplate(name = "ltrim", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Ltrim implements DrillSimpleFunc {
    @Param  VarCharHolder text;
    @Param  VarCharHolder from;

    @Output VarCharHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = text.buffer;
      out.start = out.end = text.end;

      int bytePerChar = 0;
      //Scan from left of "text", stop until find a char not in "from"
      for (int id = text.start; id < text.end; id += bytePerChar) {
        bytePerChar = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.utf8CharLen(text.buffer, id);
        int pos = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.stringLeftMatchUTF8(from.buffer, from.start, from.end,
                                                                                            text.buffer, id, id + bytePerChar);
        if (pos < 0) { // Found the 1st char not in "from", stop
          out.start = id;
          break;
        }
      }
    } // end of eval
  }

  /**
   * Remove the longest string containing only character " " from the start of "text"
   */
  @FunctionTemplate(name = "ltrim", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class LtrimOneArg implements DrillSimpleFunc {
    @Param  VarCharHolder text;

    @Output VarCharHolder out;
    @Workspace byte spaceInByte;

    @Override
    public void setup() {
      spaceInByte = 32;
    }

    @Override
    public void eval() {
      out.buffer = text.buffer;
      out.start = out.end = text.end;

      //Scan from left of "text", stop until find a char not " "
      for (int id = text.start; id < text.end; ++id) {
      if (text.buffer.getByte(id) != spaceInByte) { // Found the 1st char not " ", stop
          out.start = id;
          break;
        }
      }
    } // end of eval
  }

  /**
   * Remove the longest string containing only characters from "from"  from the end of "text"
   */
  @FunctionTemplate(name = "rtrim", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Rtrim implements DrillSimpleFunc {
    @Param  VarCharHolder text;
    @Param  VarCharHolder from;

    @Output VarCharHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = text.buffer;
      out.start = out.end = text.start;

      int bytePerChar = 0;
      //Scan from right of "text", stop until find a char not in "from"
      for (int id = text.end - 1; id >= text.start; id -= bytePerChar) {
        while ((text.buffer.getByte(id) & 0xC0) == 0x80 && id >= text.start) {
          id--;
        }
        bytePerChar = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.utf8CharLen(text.buffer, id);
        int pos = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.stringLeftMatchUTF8(from.buffer, from.start, from.end,
                                                                                            text.buffer, id, id + bytePerChar);
        if (pos < 0) { // Found the 1st char not in "from", stop
          out.end = id+ bytePerChar;
          break;
        }
      }
    } // end of eval
  }

  /**
   * Remove the longest string containing only character " " from the end of "text"
   */
  @FunctionTemplate(name = "rtrim", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RtrimOneArg implements DrillSimpleFunc {
    @Param  VarCharHolder text;

    @Output VarCharHolder out;
    @Workspace byte spaceInByte;

    @Override
    public void setup() {
      spaceInByte = 32;
    }

    @Override
    public void eval() {
      out.buffer = text.buffer;
      out.start = out.end = text.start;

      //Scan from right of "text", stop until find a char not in " "
      for (int id = text.end - 1; id >= text.start; --id) {
        while ((text.buffer.getByte(id) & 0xC0) == 0x80 && id >= text.start) {
          id--;
        }
        if (text.buffer.getByte(id) != spaceInByte) { // Found the 1st char not in " ", stop
          out.end = id + 1;
          break;
        }
      }
    } // end of eval
  }

  /**
   * Remove the longest string containing only characters from "from"  from the start of "text"
   */
  @FunctionTemplate(name = "btrim", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Btrim implements DrillSimpleFunc {
    @Param  VarCharHolder text;
    @Param  VarCharHolder from;

    @Output VarCharHolder out;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = text.buffer;
      out.start = out.end = text.start;
      int bytePerChar = 0;

      //Scan from left of "text", stop until find a char not in "from"
      for (int id = text.start; id < text.end; id += bytePerChar) {
        bytePerChar = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.utf8CharLen(text.buffer, id);
        int pos = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.stringLeftMatchUTF8(from.buffer, from.start, from.end,
                                                                                            text.buffer, id, id + bytePerChar);
        if (pos < 0) { // Found the 1st char not in "from", stop
          out.start = id;
          break;
        }
      }

      //Scan from right of "text", stop until find a char not in "from"
      for (int id = text.end - 1; id >= text.start; id -= bytePerChar) {
        while ((text.buffer.getByte(id) & 0xC0) == 0x80 && id >= text.start) {
          id--;
        }
        bytePerChar = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.utf8CharLen(text.buffer, id);
        final int pos = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.stringLeftMatchUTF8(from.buffer, from.start, from.end,
                                                                                            text.buffer, id, id + bytePerChar);
        if (pos < 0) { // Found the 1st char not in "from", stop
          out.end = id + bytePerChar;
          break;
        }
      }
    } // end of eval
  }

  /**
   * Remove the longest string containing only character " " from the start of "text"
   */
  @FunctionTemplate(name = "btrim", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class BtrimOneArg implements DrillSimpleFunc {
    @Param  VarCharHolder text;

    @Output VarCharHolder out;
    @Workspace byte spaceInByte;

    @Override
    public void setup() {
      spaceInByte = 32;
    }

    @Override
    public void eval() {
      out.buffer = text.buffer;
      out.start = out.end = text.start;

      //Scan from left of "text", stop until find a char not " "
      for (int id = text.start; id < text.end; ++id) {
        if (text.buffer.getByte(id) != spaceInByte) { // Found the 1st char not " ", stop
          out.start = id;
          break;
        }
      }

      //Scan from right of "text", stop until find a char not " "
      for (int id = text.end - 1; id >= text.start; --id) {
        while ((text.buffer.getByte(id) & 0xC0) == 0x80 && id >= text.start) {
          id--;
        }
        if (text.buffer.getByte(id) != spaceInByte) { // Found the 1st char not in " ", stop
          out.end = id + 1;
          break;
        }
      }
    } // end of eval
  }

  @FunctionTemplate(name = "split", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Split implements DrillSimpleFunc {
    @Param  VarCharHolder input;
    @Param  VarCharHolder delimiter;

    @Workspace com.google.common.base.Splitter splitter;
    @Inject DrillBuf buffer;

    @Output org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter writer;

    @Override
    public void setup() {
      int len = delimiter.end - delimiter.start;
      if (len != 1) {
        throw new IllegalArgumentException("Only single character delimiters are supported for split()");
      }
      char splitChar = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.
          toStringFromUTF8(delimiter.start, delimiter.end, delimiter.buffer).charAt(0);
      splitter = com.google.common.base.Splitter.on(splitChar);
    }

    @Override
    public void eval() {
      // Convert the iterable to an array as Janino will not handle generics.
      Object[] tokens = com.google.common.collect.Iterables.toArray(splitter.split(
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer)), String.class);
      org.apache.drill.exec.vector.complex.writer.BaseWriter.ListWriter list = writer.rootAsList();
      list.startList();
      for(int i = 0; i < tokens.length; i++ ) {
        final byte[] strBytes = ((String)tokens[i]).getBytes(com.google.common.base.Charsets.UTF_8);
        buffer = buffer.reallocIfNeeded(strBytes.length);
        buffer.setBytes(0, strBytes);
        list.varChar().writeVarChar(0, strBytes.length, buffer);
      }
      list.endList();
    }

  }

  @FunctionTemplate(name = "concatOperator", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ConcatOperator implements DrillSimpleFunc {
    @Param  VarCharHolder left;
    @Param  VarCharHolder right;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = buffer = buffer.reallocIfNeeded( (left.end - left.start) + (right.end - right.start));
      out.start = out.end = 0;

      int id = 0;
      for (id = left.start; id < left.end; id++) {
        out.buffer.setByte(out.end++, left.buffer.getByte(id));
      }

      for (id = right.start; id < right.end; id++) {
        out.buffer.setByte(out.end++, right.buffer.getByte(id));
      }
    }
  }

  //Concatenate the text representations of the arguments. NULL arguments are ignored.
  //TODO: NullHanding.INTERNAL for DrillSimpleFunc requires change in code generation.
  @FunctionTemplate(name = "concat", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class Concat implements DrillSimpleFunc {
    @Param  VarCharHolder left;
    @Param  VarCharHolder right;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = buffer = buffer.reallocIfNeeded( (left.end - left.start) + (right.end - right.start));
      out.start = out.end = 0;

      int id = 0;
      for (id = left.start; id < left.end; id++) {
        out.buffer.setByte(out.end++, left.buffer.getByte(id));
      }

      for (id = right.start; id < right.end; id++) {
        out.buffer.setByte(out.end++, right.buffer.getByte(id));
      }
    }
  }

  @FunctionTemplate(name = "concat", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class ConcatRightNullInput implements DrillSimpleFunc {
    @Param  VarCharHolder left;
    @Param  NullableVarCharHolder right;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = buffer = buffer.reallocIfNeeded( (left.end - left.start) + (right.end - right.start));;
      out.start = out.end = 0;

      int id = 0;
      for (id = left.start; id < left.end; id++) {
        out.buffer.setByte(out.end++, left.buffer.getByte(id));
      }

      if (right.isSet == 1) {
        for (id = right.start; id < right.end; id++) {
          out.buffer.setByte(out.end++, right.buffer.getByte(id));
        }
      }
    }
  }

  @FunctionTemplate(name = "concat", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class ConcatLeftNullInput implements DrillSimpleFunc {
    @Param  NullableVarCharHolder left;
    @Param  VarCharHolder right;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = buffer.reallocIfNeeded( (left.end - left.start) + (right.end - right.start));
      out.start = out.end = 0;

      int id = 0;
      if (left.isSet == 1) {
        for (id = left.start; id < left.end; id++) {
          out.buffer.setByte(out.end++, left.buffer.getByte(id));
        }
      }

      for (id = right.start; id < right.end; id++) {
        out.buffer.setByte(out.end++, right.buffer.getByte(id));
      }
    }
  }

  @FunctionTemplate(name = "concat", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class ConcatBothNullInput implements DrillSimpleFunc {
    @Param  NullableVarCharHolder left;
    @Param  NullableVarCharHolder right;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      out.buffer = buffer.reallocIfNeeded( (left.end - left.start) + (right.end - right.start));
      out.start = out.end = 0;

      int id = 0;
      if (left.isSet == 1) {
        for (id = left.start; id < left.end; id++) {
          out.buffer.setByte(out.end++, left.buffer.getByte(id));
        }
      }

      if (right.isSet == 1) {
        for (id = right.start; id < right.end; id++) {
          out.buffer.setByte(out.end++, right.buffer.getByte(id));
        }
      }
    }
  }

  // Converts a hex encoded string into a varbinary type.
  // "\xca\xfe\xba\xbe" => (byte[]) {(byte)0xca, (byte)0xfe, (byte)0xba, (byte)0xbe}
  @FunctionTemplate(name = "binary_string", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class BinaryString implements DrillSimpleFunc {
    @Param  VarCharHolder in;
    @Output VarBinaryHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      out.buffer = buffer.reallocIfNeeded(in.end - in.start);
      out.start = out.end = 0;
      out.end = org.apache.drill.common.util.DrillStringUtils.parseBinaryString(in.buffer, in.start, in.end, out.buffer);
      out.buffer.setIndex(out.start, out.end);
    }
  }

  // Converts a varbinary type into a hex encoded string.
  // (byte[]) {(byte)0xca, (byte)0xfe, (byte)0xba, (byte)0xbe}  => "\xca\xfe\xba\xbe"
  @FunctionTemplate(name = "string_binary", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class StringBinary implements DrillSimpleFunc {
    @Param  VarBinaryHolder in;
    @Output VarCharHolder   out;
    @Workspace Charset charset;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      charset = java.nio.charset.Charset.forName("UTF-8");
    }

    @Override
    public void eval() {
      byte[] buf = org.apache.drill.common.util.DrillStringUtils.toBinaryString(in.buffer, in.start, in.end).getBytes(charset);
      buffer.setBytes(0, buf);
      buffer.setIndex(0, buf.length);

      out.start = 0;
      out.end = buf.length;
      out.buffer = buffer;
    }
  }

  /**
  * Returns the ASCII code of the first character of input string
  */
  @FunctionTemplate(name = "ascii", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class AsciiString implements DrillSimpleFunc {
    @Param  VarCharHolder in;
    @Output IntHolder out;

    @Override
    public void setup() {}

    @Override
    public void eval() {
      out.value = in.buffer.getByte(in.start);
    }
  }

  /**
  * Returns the char corresponding to ASCII code input.
  */
  @FunctionTemplate(name = "chr", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class AsciiToChar implements DrillSimpleFunc {
    @Param  IntHolder in;
    @Output VarCharHolder out;
    @Inject DrillBuf buf;

    @Override
    public void setup() {
      buf = buf.reallocIfNeeded(1);
    }

    @Override
    public void eval() {
      out.buffer = buf;
      out.start = out.end = 0;
      out.buffer.setByte(0, in.value);
      ++out.end;
    }
  }

  /**
  * Returns the input char sequences repeated nTimes.
  */
  @FunctionTemplate(names = {"repeat", "repeatstr"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RepeatString implements DrillSimpleFunc {

    @Param  VarCharHolder in;
    @Param IntHolder nTimes;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      final int len = in.end - in.start;
      final int num = nTimes.value;
      out.start = 0;
      out.buffer = buffer = buffer.reallocIfNeeded( len * num );
      for (int i =0; i < num; i++) {
        in.buffer.getBytes(in.start, out.buffer, i * len, len);
      }
      out.end = len * num;
    }
  }

  /**
  * Convert string to ASCII from another encoding input.
  */
  @FunctionTemplate(name = "toascii", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class AsciiEndode implements DrillSimpleFunc {
    @Param  VarCharHolder in;
    @Param  VarCharHolder enc;
    @Output VarCharHolder out;
    @Workspace Charset inCharset;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
      inCharset = java.nio.charset.Charset.forName(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(enc.start, enc.end, enc.buffer));
    }

    @Override
    public void eval() {
      final byte[] bytea = new byte[in.end - in.start];
      int index = 0;
      for (int i = in.start; i < in.end; i++, index++) {
        bytea[index] = in.buffer.getByte(i);
      }
      final byte[] outBytea = new String(bytea, inCharset).getBytes(com.google.common.base.Charsets.UTF_8);
      out.buffer = buffer = buffer.reallocIfNeeded(outBytea.length);
      out.buffer.setBytes(0, outBytea);
      out.start = 0;
      out.end = outBytea.length;
    }
  }

  /**
  * Returns the reverse string for given input.
  */
  @FunctionTemplate(name = "reverse", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ReverseString implements DrillSimpleFunc {
    @Param  VarCharHolder in;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;

    @Override
    public void setup() {
    }

    @Override
    public void eval() {
      final int len = in.end - in.start;
      out.start = 0;
      out.end = len;
      out.buffer = buffer = buffer.reallocIfNeeded(len);
      int charLen;

      int index = out.end;
      int innerIndex;

      for (int id = in.start; id < in.end; id += charLen) {
        innerIndex = charLen = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.utf8CharLen(in.buffer, id);

        while (innerIndex > 0) {
          out.buffer.setByte(index - innerIndex, in.buffer.getByte(id + (charLen - innerIndex)));
          innerIndex--;
        }

        index -= charLen;
      }
    }
  }
}
