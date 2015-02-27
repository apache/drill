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
import org.apache.drill.exec.record.RecordBatch;

public class StringFunctions{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StringFunctions.class);

  private StringFunctions() {}

  /*
   * String Function Implementation.
   */

  @FunctionTemplate(name = "like", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Like implements DrillSimpleFunc{

    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Output BitHolder out;
    @Workspace java.util.regex.Matcher matcher;

    public void setup(RecordBatch incoming) {
      matcher = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.RegexpUtil.sqlToRegexLike( //
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(pattern.start,  pattern.end,  pattern.buffer))).matcher("");
    }

    public void eval() {
      String i = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer);
      matcher.reset(i);
      out.value = matcher.matches()? 1:0;
    }
  }

  @FunctionTemplate(name = "like", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class LikeWithEscape implements DrillSimpleFunc{

    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Param(constant=true) VarCharHolder escape;
    @Output BitHolder out;
    @Workspace java.util.regex.Matcher matcher;

    public void setup(RecordBatch incoming) {
      matcher = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.RegexpUtil.sqlToRegexLike( //
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(pattern.start,  pattern.end,  pattern.buffer),
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(escape.start,  escape.end,  escape.buffer))).matcher("");
    }

    public void eval() {
      String i = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer);
      matcher.reset(i);
      out.value = matcher.matches()? 1:0;
    }
  }

  @FunctionTemplate(names = {"similar", "similar_to"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Similar implements DrillSimpleFunc{
    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Output BitHolder out;
    @Workspace java.util.regex.Matcher matcher;

    public void setup(RecordBatch incoming) {

      matcher = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.RegexpUtil.sqlToRegexSimilar(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(pattern.start,  pattern.end,  pattern.buffer))).matcher("");
    }

    public void eval() {
      String i = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer);
      matcher.reset(i);
      out.value = matcher.matches()? 1:0;
    }
  }

  @FunctionTemplate(names = {"similar", "similar_to"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class SimilarWithEscape implements DrillSimpleFunc{
    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Param(constant=true) VarCharHolder escape;
    @Output BitHolder out;
    @Workspace java.util.regex.Matcher matcher;

    public void setup(RecordBatch incoming) {

      matcher = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.RegexpUtil.sqlToRegexSimilar(
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(pattern.start,  pattern.end,  pattern.buffer),
          org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(escape.start,  escape.end,  escape.buffer))).matcher("");
    }

    public void eval() {
      String i = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer);
      matcher.reset(i);
      out.value = matcher.matches()? 1:0;
    }
  }

  /*
   * Replace all substring that match the regular expression with replacement.
   */
  @FunctionTemplate(name = "regexp_replace", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class RegexpReplace implements DrillSimpleFunc{

    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Param VarCharHolder replacement;
    @Inject DrillBuf buffer;
    @Workspace java.util.regex.Matcher matcher;
    @Output VarCharHolder out;

    public void setup(RecordBatch incoming) {
      matcher = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(pattern.start,  pattern.end,  pattern.buffer)).matcher("");
    }

    public void eval() {

      out.start = 0;
      String i = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(input.start, input.end, input.buffer);
      String r = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(replacement.start, replacement.end, replacement.buffer);
      byte [] bytea = matcher.reset(i).replaceAll(r).getBytes(java.nio.charset.Charset.forName("UTF-8"));
      out.buffer = buffer = buffer.reallocIfNeeded(bytea.length);
      out.buffer.setBytes(out.start, bytea);
      out.end = bytea.length;
    }
  }

  @FunctionTemplate(names = {"char_length", "character_length", "length"}, scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class CharLength implements DrillSimpleFunc{

    @Param  VarCharHolder input;
    @Output BigIntHolder out;

    public void setup(RecordBatch incoming) {}

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(input.buffer, input.start, input.end);
    }
  }

  @FunctionTemplate(name = "lengthUtf8", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ByteLength implements DrillSimpleFunc{

    @Param  VarBinaryHolder input;
    @Output BigIntHolder out;

    public void setup(RecordBatch incoming) {}

    public void eval() {
      out.value = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(input.buffer, input.start, input.end);
    }
  }

  @FunctionTemplate(name = "octet_length", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class OctetLength implements DrillSimpleFunc{

    @Param  VarCharHolder input;
    @Output BigIntHolder out;

    public void setup(RecordBatch incoming) {}

    public void eval() {
      out.value = input.end - input.start;
    }
  }

  @FunctionTemplate(name = "bit_length", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class BitLength implements DrillSimpleFunc{

    @Param  VarCharHolder input;
    @Output BigIntHolder out;

    public void setup(RecordBatch incoming) {}

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
  public static class Position implements DrillSimpleFunc{

    @Param  VarCharHolder substr;
    @Param  VarCharHolder str;

    @Output BigIntHolder out;

    public void setup(RecordBatch incoming) {}

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

  // same as function "position(substr, str) ", except the reverse order of argument.
  @FunctionTemplate(name = "strpos", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Strpos implements DrillSimpleFunc{

    @Param  VarCharHolder str;
    @Param  VarCharHolder substr;

    @Output BigIntHolder out;

    public void setup(RecordBatch incoming) {}

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
  public static class LowerCase implements DrillSimpleFunc{

    @Param VarCharHolder input;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;

    public void setup(RecordBatch incoming) {
    }

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
  public static class UpperCase implements DrillSimpleFunc{

    @Param VarCharHolder input;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      out.buffer = buffer = buffer.reallocIfNeeded(input.end- input.start);
      out.start = 0;
      out.end = input.end - input.start;

      for (int id = input.start; id < input.end; id++) {
        byte  currentByte = input.buffer.getByte(id);

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
  public static class Substring implements DrillSimpleFunc{

    @Param VarCharHolder string;
    @Param BigIntHolder offset;
    @Param BigIntHolder length;

    @Output VarCharHolder out;
    @Workspace ByteBuf buffer;

    public void setup(RecordBatch incoming) {

    }

    public void eval() {
      out.buffer = string.buffer;
      // if length is NOT positive, or offset is NOT positive, or input string is empty, return empty string.
      if (length.value <= 0 || offset.value <=0 || string.end <= string.start) {
        out.start = out.end = 0;
      } else {
        //Do 1st scan to counter # of character in string.
        int charCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(string.buffer, string.start, string.end);

        int fromCharIdx = (int) offset.value; //the start position of char  (inclusive)

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
  public static class SubstringOffset implements DrillSimpleFunc{

    @Param VarCharHolder string;
    @Param BigIntHolder offset;

    @Output VarCharHolder out;
    @Workspace ByteBuf buffer;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      out.buffer = string.buffer;
      // if length is NOT positive, or offset is NOT positive, or input string is empty, return empty string.
      if (offset.value <=0 || string.end <= string.start) {
        out.start = out.end = 0;
      } else {
        //Do 1st scan to counter # of character in string.
        int charCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(string.buffer, string.start, string.end);

        int fromCharIdx = (int) offset.value; //the start position of char  (inclusive)

        if (fromCharIdx > charCount ) { // invalid length, return empty string.
          out.start = out.end = 0;
        } else {
          out.start = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(string.buffer, string.start, string.end, fromCharIdx-1);
          out.end = string.end;
        }
      }
    }

  }

  // Return first length characters in the string. When length is negative, return all but last |length| characters.
  // If length > total charcounts, return the whole string.
  // If length = 0, return empty
  // If length < 0, and |length| > total charcounts, return empty.
  @FunctionTemplate(name = "left", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Left implements DrillSimpleFunc{

    @Param VarCharHolder string;
    @Param BigIntHolder length;

    @Output VarCharHolder out;
    @Workspace ByteBuf buffer;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      out.buffer = string.buffer;
      // if length is 0, or input string is empty, return empty string.
      if (length.value == 0 || string.end <= string.start) {
        out.start = out.end = 0;
      } else {
        //Do 1st scan to counter # of character in string.
        int charCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(string.buffer, string.start, string.end);

        int charLen = 0;
        if (length.value > 0) {
          charLen = Math.min((int)length.value, charCount);  //left('abc', 5) -> 'abc'
        } else if (length.value < 0) {
          charLen = Math.max(0, charCount + (int)length.value) ; // left('abc', -5) ==> ''
        }

        out.start = string.start; //Starting from the left of input string.
        out.end = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(string.buffer, out.start, string.end, charLen);
      } // end of lenth.value != 0
    }
  }

  //Return last 'length' characters in the string. When 'length' is negative, return all but first |length| characters.
  @FunctionTemplate(name = "right", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Right implements DrillSimpleFunc{

    @Param VarCharHolder string;
    @Param BigIntHolder length;

    @Output VarCharHolder out;
    @Workspace ByteBuf buffer;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      out.buffer = string.buffer;
      // invalid length.
      if (length.value == 0 || string.end <= string.start) {
        out.start = out.end = 0;
      } else {
        //Do 1st scan to counter # of character in string.
        int charCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(string.buffer, string.start, string.end);

        int fromCharIdx; //the start position of char (inclusive)
        int charLen; // the end position of char (inclusive)
        if (length.value > 0) {
          fromCharIdx = Math.max(charCount - (int) length.value + 1, 1); // right('abc', 5) ==> 'abc' fromCharIdx=1.
          charLen = charCount - fromCharIdx + 1;
        } else { // length.value < 0
          fromCharIdx = Math.abs((int) length.value) + 1;
          charLen = charCount - fromCharIdx +1;
        }

        // invalid length :  right('abc', -5) -> ''
        if (charLen <=0) {
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
  public static class InitCap implements DrillSimpleFunc{

    @Param VarCharHolder input;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      out.buffer = buffer = buffer.reallocIfNeeded(input.end - input.start);
      out.start = 0;
      out.end = input.end - input.start;
      org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.initCap(input.start, input.end, input.buffer, out.buffer);
    }

  }

  //Replace all occurrences in 'text' of substring 'from' with substring 'to'
  @FunctionTemplate(name = "replace", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Replace implements DrillSimpleFunc{

    @Param  VarCharHolder text;
    @Param  VarCharHolder from;
    @Param  VarCharHolder to;
    @Inject DrillBuf buffer;
    @Output VarCharHolder out;

    public void setup(RecordBatch incoming) {
      buffer = buffer.reallocIfNeeded(8000);
    }

    public void eval() {
      out.buffer = buffer;
      out.start = out.end = 0;
      int fromL = from.end - from.start;
      int textL = text.end - text.start;

      if (fromL > 0 && fromL <= textL) {
        //If "from" is not empty and it's length is no longer than text's length
        //then, we may find a match, and do replace.
        int i = text.start;
        for (; i<=text.end - fromL; ) {
          int j = from.start;
          for (; j<from.end; j++) {
            if (text.buffer.getByte(i + j - from.start) != from.buffer.getByte(j)) {
              break;
            }
          }

          if (j == from.end ) {
            //find a true match ("from" is not empty), copy entire "to" string to out buffer
            for (int k = to.start ; k< to.end; k++) {
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
        for (; i< text.end; i++) {
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
  public static class Lpad implements DrillSimpleFunc{

    @Param  VarCharHolder text;
    @Param  BigIntHolder length;
    @Param  VarCharHolder fill;
    @Inject DrillBuf buffer;

    @Output VarCharHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      buffer = buffer.reallocIfNeeded((int) length.value*2);
      byte currentByte = 0;
      int id = 0;
      //get the char length of text.
      int textCharCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(text.buffer, text.start, text.end);

      //get the char length of fill.
      int fillCharCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(fill.buffer, fill.start, fill.end);

      if (length.value <= 0) {
        //case 1: target length is <=0, then return an empty string.
        out.buffer = buffer;
        out.start = out.end = 0;
      } else if (length.value == textCharCount || (length.value > textCharCount  && fillCharCount == 0) ) {
        //case 2: target length is same as text's length, or need fill into text but "fill" is empty, then return text directly.
        out.buffer = text.buffer;
        out.start = text.start;
        out.end = text.end;
      } else if (length.value < textCharCount) {
        //case 3: truncate text on the right side. It's same as substring(text, 1, length).
        out.buffer = text.buffer;
        out.start = text.start;
        out.end = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(text.buffer, text.start, text.end, (int)length.value);
      } else if (length.value > textCharCount) {
        //case 4: copy "fill" on left. Total # of char to copy : length.value - textCharCount
        int count = 0;
        out.buffer = buffer;
        out.start = out.end = 0;

        while (count < length.value - textCharCount) {
          for (id = fill.start; id < fill.end; id++) {
            if (count == length.value - textCharCount) {
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

  /**
   * Fill up the string to length "length" by appending the characters 'fill' at the end of 'text'
   * If the string is already longer than length then it is truncated.
   */
  @FunctionTemplate(name = "rpad", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Rpad implements DrillSimpleFunc{

    @Param  VarCharHolder text;
    @Param  BigIntHolder length;
    @Param  VarCharHolder fill;
    @Inject DrillBuf buffer;

    @Output VarCharHolder out;

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      buffer = buffer.reallocIfNeeded((int) length.value*2);

      byte currentByte = 0;
      int id = 0;
      //get the char length of text.
      int textCharCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(text.buffer, text.start, text.end);

      //get the char length of fill.
      int fillCharCount = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(fill.buffer, fill.start, fill.end);

      if (length.value <= 0) {
        //case 1: target length is <=0, then return an empty string.
        out.buffer = buffer;
        out.start = out.end = 0;
      } else if (length.value == textCharCount || (length.value > textCharCount  && fillCharCount == 0) ) {
        //case 2: target length is same as text's length, or need fill into text but "fill" is empty, then return text directly.
        out.buffer = text.buffer;
        out.start = text.start;
        out.end = text.end;
      } else if (length.value < textCharCount) {
        //case 3: truncate text on the right side. It's same as substring(text, 1, length).
        out.buffer = text.buffer;
        out.start = text.start;
        out.end = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(text.buffer, text.start, text.end, (int)length.value);
      } else if (length.value > textCharCount) {
        //case 4: copy "text" into "out", then copy "fill" on the right.
        out.buffer = buffer;
        out.start = out.end = 0;

        for (id = text.start; id < text.end; id++) {
          out.buffer.setByte(out.end++, text.buffer.getByte(id));
        }

        //copy "fill" on right. Total # of char to copy : length.value - textCharCount
        int count = 0;

        while (count < length.value - textCharCount) {
          for (id = fill.start; id < fill.end; id++) {
            if (count == length.value - textCharCount) {
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
   * Remove the longest string containing only characters from "from"  from the start of "text"
   */
  @FunctionTemplate(name = "ltrim", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Ltrim implements DrillSimpleFunc{

    @Param  VarCharHolder text;
    @Param  VarCharHolder from;

    @Output VarCharHolder out;

    public void setup(RecordBatch incoming) {
    }

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
   * Remove the longest string containing only characters from "from"  from the end of "text"
   */
  @FunctionTemplate(name = "rtrim", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Rtrim implements DrillSimpleFunc{

    @Param  VarCharHolder text;
    @Param  VarCharHolder from;

    @Output VarCharHolder out;

    public void setup(RecordBatch incoming) {
    }

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
   * Remove the longest string containing only characters from "from"  from the start of "text"
   */
  @FunctionTemplate(name = "btrim", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Btrim implements DrillSimpleFunc{

    @Param  VarCharHolder text;
    @Param  VarCharHolder from;

    @Output VarCharHolder out;

    public void setup(RecordBatch incoming) {
    }

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
        int pos = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.stringLeftMatchUTF8(from.buffer, from.start, from.end,
                                                                                            text.buffer, id, id + bytePerChar);
        if (pos < 0) { // Found the 1st char not in "from", stop
          out.end = id+ bytePerChar;
          break;
        }
      }
    } // end of eval
  }

  @FunctionTemplate(name = "concatOperator", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class ConcatOperator implements DrillSimpleFunc{
    @Param  VarCharHolder left;
    @Param  VarCharHolder right;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;

    public void setup(RecordBatch incoming) {
    }

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
  public static class Concat implements DrillSimpleFunc{

    @Param  VarCharHolder left;
    @Param  VarCharHolder right;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;


    public void setup(RecordBatch incoming) {
    }

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
  public static class ConcatRightNullInput implements DrillSimpleFunc{

    @Param  VarCharHolder left;
    @Param  NullableVarCharHolder right;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;


    public void setup(RecordBatch incoming) {
    }

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
  public static class ConcatLeftNullInput implements DrillSimpleFunc{

    @Param  NullableVarCharHolder left;
    @Param  VarCharHolder right;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;


    public void setup(RecordBatch incoming) {
    }

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
  public static class ConcatBothNullInput implements DrillSimpleFunc{

    @Param  NullableVarCharHolder left;
    @Param  NullableVarCharHolder right;
    @Output VarCharHolder out;
    @Inject DrillBuf buffer;


    public void setup(RecordBatch incoming) {
    }

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

    public void setup(RecordBatch incoming) { }

    public void eval() {
      out.buffer = in.buffer;
      out.start = in.start;
      out.end = org.apache.drill.common.util.DrillStringUtils.parseBinaryString(in.buffer, in.start, in.end);
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

    public void setup(RecordBatch incoming) {
      charset = java.nio.charset.Charset.forName("UTF-8");
    }

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

    public void setup(RecordBatch incoming) { }

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

    public void setup(RecordBatch incoming) {
      buf = buf.reallocIfNeeded(1);
    }

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

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      final int len = in.end - in.start;
      int num = nTimes.value;
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

    public void setup(RecordBatch incoming) {
      inCharset = java.nio.charset.Charset.forName(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.toStringFromUTF8(enc.start, enc.end, enc.buffer));
    }

    public void eval() {
      byte[] bytea = new byte[in.end - in.start];
      int index =0;
      for (int i = in.start; i<in.end; i++, index++) {
        bytea[index]=in.buffer.getByte(i);
      }
      byte[] outBytea = new String(bytea, inCharset).getBytes(com.google.common.base.Charsets.UTF_8);
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

    public void setup(RecordBatch incoming) {
    }

    public void eval() {
      final int len = in.end - in.start;
      out.start = 0;
      out.end = len;
      out.buffer = buffer = buffer.reallocIfNeeded(len);
      int charlen = 0;

      int index = in.end;
      int innerindex = 0;

      for (int id = in.start; id < in.end; id+=charlen) {
        innerindex = charlen = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.utf8CharLen(in.buffer, id);

        while (innerindex > 0) {
          out.buffer.setByte(index - innerindex, in.buffer.getByte(id + (charlen - innerindex)));
          innerindex-- ;
        }

        index -= charlen;
      }
    }
  }

}
