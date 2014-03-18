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

import org.apache.drill.common.expression.Arg;
import org.apache.drill.common.expression.ArgumentValidators.*;
import org.apache.drill.common.expression.BasicArgumentValidator;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;

import com.fasterxml.jackson.databind.ser.std.NumberSerializers.IntegerSerializer;

public class StringFunctions{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StringFunctions.class);
  
  private StringFunctions(){}
  
  /*
   * String Function Implementation. 
   */
  
  @FunctionTemplate(name = "like", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Like implements DrillSimpleFunc{
    
    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Output BitHolder out;
    @Workspace java.util.regex.Pattern regPattern;
    
    public void setup(RecordBatch incoming){
      regPattern = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.RegexpUtil.sqlToRegexLike(pattern.toString()));
    }
    
    public void eval(){
      out.value = regPattern.matcher(input.toString()).matches()? 1:0;
    }
  }

  @FunctionTemplate(name = "similar", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Similar implements DrillSimpleFunc{    
    @Param VarCharHolder input;
    @Param(constant=true) VarCharHolder pattern;
    @Output BitHolder out;
    @Workspace java.util.regex.Pattern regPattern;

    public void setup(RecordBatch incoming){
      regPattern = java.util.regex.Pattern.compile(org.apache.drill.exec.expr.fn.impl.RegexpUtil.sqlToRegexSimilar(pattern.toString()));      
    }
    
    public void eval(){
      out.value = regPattern.matcher(input.toString()).matches()? 1:0;
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
    @Workspace ByteBuf buffer;
    @Workspace java.util.regex.Pattern regPattern;    
    @Output VarCharHolder out;

    public void setup(RecordBatch incoming){
      buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte [8000]);  
      regPattern = java.util.regex.Pattern.compile(pattern.toString());
    }
    
    public void eval(){
      out.buffer = buffer;
      out.start = 0;
      
      byte [] bytea = regPattern.matcher(input.toString()).replaceAll(replacement.toString()).getBytes(java.nio.charset.Charset.forName("UTF-8"));
      out.buffer.setBytes(out.start, bytea);
      out.end = bytea.length;
    }
  }
  

  @FunctionTemplate(name = "char_length", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class CharLength implements DrillSimpleFunc{
    
    @Param  VarCharHolder input;
    @Output BigIntHolder out;

    public void setup(RecordBatch incoming){}
    
    public void eval(){
      out.value = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharLength(input.buffer, input.start, input.end);          
    } 
    
  }

  @FunctionTemplate(name = "octet_length", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class OctetLength implements DrillSimpleFunc{
    
    @Param  VarCharHolder input;
    @Output BigIntHolder out;

    public void setup(RecordBatch incoming){}
    
    public void eval(){  
      out.value = input.end - input.start;
    }
  }

  @FunctionTemplate(name = "bit_length", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class BitLength implements DrillSimpleFunc{
    
    @Param  VarCharHolder input;
    @Output BigIntHolder out;

    public void setup(RecordBatch incoming){}
    
    public void eval(){  
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

    public void setup(RecordBatch incoming){}
    
    public void eval(){
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

    public void setup(RecordBatch incoming){}
    
    public void eval(){  
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
    @Workspace ByteBuf buffer;     

    public void setup(RecordBatch incoming){
      buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte [8000]);
    }
    
    public void eval(){
      out.buffer = buffer;
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
    @Workspace ByteBuf buffer;     

    public void setup(RecordBatch incoming){
      buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte [8000]);
    }
    
    public void eval() {
      out.buffer = buffer;
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
  @FunctionTemplate(name = "substring", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
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

    public void setup(RecordBatch incoming){
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

    public void setup(RecordBatch incoming){
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
    @Workspace ByteBuf buffer;     

    public void setup(RecordBatch incoming){
      buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte [8000]);
    }
    
    public void eval() {
      out.buffer = buffer;
      out.start = 0;
      out.end = input.end - input.start;
           
      // Assumes Alpha as [A-Za-z0-9]
      // white space is treated as everything else.      
      boolean capNext = true;
      for (int id = input.start; id < input.end; id++) {
        byte  currentByte = input.buffer.getByte(id);
        
        // 'A - Z' : 0x41 - 0x5A
        // 'a - z' : 0x61 - 0x7A
        // '0-9'   : 0x30 - 0x39
        if (capNext) {  // curCh is whitespace or first character of word.
          if (currentByte >= 0x30 && currentByte <= 0x39) { // 0-9
            capNext = false;
          } else if (currentByte >=0x41 && currentByte <= 0x5A) {  // A-Z
            capNext = false;
          } else if (currentByte >= 0x61 && currentByte <= 0x7A) {  // a-z
            capNext = false;
            currentByte -= 0x20; // Uppercase this character
          }
          // else {} whitespace
        } else {  // Inside of a word or white space after end of word.
          if (currentByte >= 0x30 && currentByte <= 0x39) { // 0-9
            // noop
          } else if (currentByte >=0x41 && currentByte <= 0x5A) {  // A-Z
            currentByte -= 0x20 ; // Lowercase this character
          } else if (currentByte >= 0x61 && currentByte <= 0x7A) {  // a-z
            // noop
          } else { // whitespace
            capNext = true;
          }
        }
        
        out.buffer.setByte(id - input.start, currentByte) ;
      } //end of for_loop
      
    }
    
  }
  
  //Replace all occurrences in 'text' of substring 'from' with substring 'to'
  @FunctionTemplate(name = "replace", scope = FunctionScope.SIMPLE, nulls = NullHandling.NULL_IF_NULL)
  public static class Replace implements DrillSimpleFunc{
    
    @Param  VarCharHolder text;
    @Param  VarCharHolder from;
    @Param  VarCharHolder to;
    @Workspace ByteBuf buffer;     
    @Output VarCharHolder out;

    public void setup(RecordBatch incoming){
      buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte [8000]);
    }
    
    public void eval(){
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
            if (text.buffer.getByte(i + j - from.start) != from.buffer.getByte(j))
              break;
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
    @Workspace ByteBuf buffer;     
    
    @Output VarCharHolder out;

    public void setup(RecordBatch incoming){
      buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte [8000]);
    }
    
    public void eval() {      
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
            if (count == length.value - textCharCount)
              break;
            
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
        for (id = text.start; id < text.end; id++) 
          out.buffer.setByte(out.end++, text.buffer.getByte(id));
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
    @Workspace ByteBuf buffer;     
    
    @Output VarCharHolder out;

    public void setup(RecordBatch incoming){
      buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte [8000]);
    }
    
    public void eval() {      
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
        //case 3: truncate text on left side, by (textCharCount - length.value) chars. 
        out.buffer = text.buffer;
        out.start = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.getUTF8CharPosition(text.buffer, text.start, text.end, (int) (textCharCount - length.value));
        out.end = text.end;
      } else if (length.value > textCharCount) {        
        //case 4: copy "text" into "out", then copy "fill" on the right.
        out.buffer = buffer;
        out.start = out.end = 0;

        for (id = text.start; id < text.end; id++) 
          out.buffer.setByte(out.end++, text.buffer.getByte(id));

        //copy "fill" on right. Total # of char to copy : length.value - textCharCount
        int count = 0;
        
        while (count < length.value - textCharCount) {
          for (id = fill.start; id < fill.end; id++) {
            if (count == length.value - textCharCount)
              break;
            
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

    public void setup(RecordBatch incoming){
    }
    
    public void eval() {      
      out.buffer = text.buffer;
      out.start = out.end = text.end; 

      byte currentByte = 0;
      int id = 0;
      int bytePerChar = 0;
      //Scan from left of "text", stop until find a char not in "from"
      for (id = text.start; id < text.end; ) {
        currentByte = text.buffer.getByte(id);
        
        bytePerChar = 0;
        
        if (currentByte < 0x128)                 // 1-byte char. First byte is 0xxxxxxx.
          bytePerChar = 1;
        else if ((currentByte & 0xE0) == 0xC0 )   // 2-byte char. First byte is 110xxxxx
          bytePerChar = 2;
        else if ((currentByte & 0xF0) == 0xE0 )   // 3-byte char. First byte is 1110xxxx 
          bytePerChar = 3;
        else if ((currentByte & 0xF8) == 0xF0)    //4-byte char. First byte is 11110xxx
          bytePerChar = 4;
        
        //Scan to check if "from" contains the character of "byterPerChar" bytes.
        int pos = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.stringLeftMatchUTF8(from.buffer, from.start, from.end,
                                                                                            text.buffer, id, id + bytePerChar);
        if (pos < 0) { // Found the 1st char not in "from", stop
          out.start = id; 
          break;
        }
        id += bytePerChar; //Advance to next character.  
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

    public void setup(RecordBatch incoming){
    }
    
    public void eval() {      
      out.buffer = text.buffer;
      out.start = out.end = text.start; 

      byte currentByte = 0;
      int id = 0;
      int bytePerChar = 0;
      //Scan from right of "text", stop until find a char not in "from"
      for (id = text.end-1; id>=  text.start; ) {
        currentByte = text.buffer.getByte(id);
        
        bytePerChar = 0;
        //In UTF-8 encoding, the continuation byte for a multi-byte char is 10xxxxxx.
        //Continue back-off to prior byte if it's continuation byte
        if ( (currentByte & 0xC0) == 0x80) {
          id --;
          continue;
        } else if (currentByte < 0x128)                 // 1-byte char. First byte is 0xxxxxxx.
          bytePerChar = 1;
        else if ((currentByte & 0xE0) == 0xC0 )   // 2-byte char. First byte is 110xxxxx
          bytePerChar = 2;
        else if ((currentByte & 0xF0) == 0xE0 )   // 3-byte char. First byte is 1110xxxx 
          bytePerChar = 3;
        else if ((currentByte & 0xF8) == 0xF0)    //4-byte char. First byte is 11110xxx
          bytePerChar = 4;
        
        //Scan to check if "from" contains the character of "byterPerChar" bytes. The lead byte starts at id.  
        int pos = org.apache.drill.exec.expr.fn.impl.StringFunctionUtil.stringLeftMatchUTF8(from.buffer, from.start, from.end,
                                                                                            text.buffer, id, id + bytePerChar);
        if (pos < 0) { // Found the 1st char not in "from", stop
          out.end = id+ bytePerChar; 
          break;
        }
 
        id --; // back-off to prior character.  
      }
    } // end of eval
  }

  //Concatenate the text representations of the arguments. NULL arguments are ignored.
  //TODO: NullHanding.INTERNAL for DrillSimpleFunc requires change in code generation. 
  @FunctionTemplate(name = "concat", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class Concat implements DrillSimpleFunc{
    
    @Param  VarCharHolder left;
    @Param  VarCharHolder right;
    @Output VarCharHolder out;
    @Workspace ByteBuf buffer;     
    
    
    public void setup(RecordBatch incoming){
      buffer = io.netty.buffer.Unpooled.wrappedBuffer(new byte [8000]);
    }
    
    public void eval(){
      out.buffer = buffer;
      out.start = out.end = 0;
      
      int id = 0;
      for (id = left.start; id < left.end; id++) 
        out.buffer.setByte(out.end++, left.buffer.getByte(id));
      
      for (id = right.start; id < right.end; id++)
        out.buffer.setByte(out.end++, right.buffer.getByte(id));
    } 
    
  }

  
  /*
   * Function Definitions
   */
  
  public static class Provider implements CallProvider {

    @Override
    public FunctionDefinition[] getFunctionDefintions() {
      return new FunctionDefinition[] {
          FunctionDefinition.simple("like", new AllowedTypeList(2, Types.required(MinorType.VARCHAR), Types.optional(MinorType.VARCHAR)), new OutputTypeDeterminer.NullIfNullType(MinorType.BIT), "like"),   
          FunctionDefinition.simple("similar", new AllowedTypeList(2, Types.required(MinorType.VARCHAR), Types.optional(MinorType.VARCHAR)), new OutputTypeDeterminer.NullIfNullType(MinorType.BIT), "similar"),
          FunctionDefinition.simple("regexp_replace", new AllowedTypeList(3, Types.required(MinorType.VARCHAR), Types.optional(MinorType.VARCHAR)), new OutputTypeDeterminer.NullIfNullType(MinorType.VARCHAR), "regexp_replace"),
          FunctionDefinition.simple("char_length", new AllowedTypeList(1, Types.required(MinorType.VARCHAR), Types.optional(MinorType.VARCHAR)), new OutputTypeDeterminer.NullIfNullType(MinorType.BIGINT), "char_length","character_length","length"),
          FunctionDefinition.simple("octet_length", new AllowedTypeList(1, Types.required(MinorType.VARCHAR), Types.optional(MinorType.VARCHAR)), new OutputTypeDeterminer.NullIfNullType(MinorType.BIGINT), "octet_length"),
          FunctionDefinition.simple("bit_length", new AllowedTypeList(1, Types.required(MinorType.VARCHAR), Types.optional(MinorType.VARCHAR)), new OutputTypeDeterminer.NullIfNullType(MinorType.BIGINT), "bit_length"),     
          FunctionDefinition.simple("position", new AllowedTypeList(2, Types.required(MinorType.VARCHAR), Types.optional(MinorType.VARCHAR)), new OutputTypeDeterminer.NullIfNullType(MinorType.BIGINT), "position"), 
          FunctionDefinition.simple("strpos", new AllowedTypeList(2, Types.required(MinorType.VARCHAR), Types.optional(MinorType.VARCHAR)), new OutputTypeDeterminer.NullIfNullType(MinorType.BIGINT), "strpos"),           
          FunctionDefinition.simple("lower", new AllowedTypeList(1, Types.required(MinorType.VARCHAR), Types.optional(MinorType.VARCHAR)), new OutputTypeDeterminer.NullIfNullType(MinorType.VARCHAR), "lower"),
          FunctionDefinition.simple("upper", new AllowedTypeList(1, Types.required(MinorType.VARCHAR), Types.optional(MinorType.VARCHAR)), new OutputTypeDeterminer.NullIfNullType(MinorType.VARCHAR), "upper"),   
          FunctionDefinition.simple("initcap", new AllowedTypeList(1, Types.required(MinorType.VARCHAR), Types.optional(MinorType.VARCHAR)), new OutputTypeDeterminer.NullIfNullType(MinorType.VARCHAR), "initcap"),   
          FunctionDefinition.simple("substring", new BasicArgumentValidator(new Arg(Types.required(TypeProtos.MinorType.VARCHAR), Types.optional(TypeProtos.MinorType.VARCHAR)),
                                                                            new Arg(Types.required(TypeProtos.MinorType.BIGINT),  Types.optional(TypeProtos.MinorType.BIGINT)),
                                                                            new Arg(Types.required(TypeProtos.MinorType.BIGINT),  Types.optional(TypeProtos.MinorType.BIGINT))                                                
                                                                           ),
                                   new OutputTypeDeterminer.NullIfNullType(MinorType.VARCHAR),
                                   "substring",
                                   "substr"),
          FunctionDefinition.simple("left", new BasicArgumentValidator(new Arg(Types.required(TypeProtos.MinorType.VARCHAR), Types.optional(TypeProtos.MinorType.VARCHAR)),                                            
                                                                       new Arg(Types.required(TypeProtos.MinorType.BIGINT),  Types.optional(TypeProtos.MinorType.BIGINT))                                                
                                                                      ),
                                   new OutputTypeDeterminer.NullIfNullType(MinorType.VARCHAR),
                                   "left"),
          FunctionDefinition.simple("right",new BasicArgumentValidator(new Arg(Types.required(TypeProtos.MinorType.VARCHAR), Types.optional(TypeProtos.MinorType.VARCHAR)),                                            
                                                                       new Arg(Types.required(TypeProtos.MinorType.BIGINT),  Types.optional(TypeProtos.MinorType.BIGINT))                                                
                                                                      ),
                                   new OutputTypeDeterminer.NullIfNullType(MinorType.VARCHAR),
                                   "right"),
          FunctionDefinition.simple("replace",new BasicArgumentValidator(new Arg(Types.required(TypeProtos.MinorType.VARCHAR), Types.optional(TypeProtos.MinorType.VARCHAR)),
                                                                         new Arg(Types.required(TypeProtos.MinorType.VARCHAR), Types.optional(TypeProtos.MinorType.VARCHAR)),
                                                                         new Arg(Types.required(TypeProtos.MinorType.VARCHAR), Types.optional(TypeProtos.MinorType.VARCHAR))
                                                                        ),                                                 
                                   new OutputTypeDeterminer.NullIfNullType(MinorType.VARCHAR),
                                   "replace"),
          FunctionDefinition.simple("lpad",new BasicArgumentValidator(new Arg(Types.required(TypeProtos.MinorType.VARCHAR), Types.optional(TypeProtos.MinorType.VARCHAR)),
                                                                      new Arg(Types.required(TypeProtos.MinorType.BIGINT), Types.optional(TypeProtos.MinorType.BIGINT)),
                                                                      new Arg(Types.required(TypeProtos.MinorType.VARCHAR), Types.optional(TypeProtos.MinorType.VARCHAR))
                                                                     ),                                                 
                                   new OutputTypeDeterminer.NullIfNullType(MinorType.VARCHAR),
                                   "lpad") ,
          FunctionDefinition.simple("rpad",new BasicArgumentValidator(new Arg(Types.required(TypeProtos.MinorType.VARCHAR), Types.optional(TypeProtos.MinorType.VARCHAR)),
                                                                      new Arg(Types.required(TypeProtos.MinorType.BIGINT), Types.optional(TypeProtos.MinorType.BIGINT)),
                                                                      new Arg(Types.required(TypeProtos.MinorType.VARCHAR), Types.optional(TypeProtos.MinorType.VARCHAR))
                                                                      ),                                                 
                                   new OutputTypeDeterminer.NullIfNullType(MinorType.VARCHAR),
                                   "rpad"),
          FunctionDefinition.simple("ltrim",new BasicArgumentValidator(new Arg(Types.required(TypeProtos.MinorType.VARCHAR), Types.optional(TypeProtos.MinorType.VARCHAR)),
                                                                       new Arg(Types.required(TypeProtos.MinorType.VARCHAR), Types.optional(TypeProtos.MinorType.VARCHAR))
                                                                      ),                                                 
                                   new OutputTypeDeterminer.NullIfNullType(MinorType.VARCHAR),
                                    "ltrim"),
          FunctionDefinition.simple("rtrim",new BasicArgumentValidator(new Arg(Types.required(TypeProtos.MinorType.VARCHAR), Types.optional(TypeProtos.MinorType.VARCHAR)),
                                                                       new Arg(Types.required(TypeProtos.MinorType.VARCHAR), Types.optional(TypeProtos.MinorType.VARCHAR))
                                                                      ),                                                 
                                   new OutputTypeDeterminer.NullIfNullType(MinorType.VARCHAR), "rtrim")  ,
         FunctionDefinition.simple("concat", new AllowedTypeList(2, Types.required(MinorType.VARCHAR), Types.optional(MinorType.VARCHAR)), new OutputTypeDeterminer.NullIfNullType(MinorType.VARCHAR), "concat"),
                                   
      };
    }
  }
}