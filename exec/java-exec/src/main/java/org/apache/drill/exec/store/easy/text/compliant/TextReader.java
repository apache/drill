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
package org.apache.drill.exec.store.easy.text.compliant;

import io.netty.buffer.DrillBuf;

import java.io.IOException;

import org.apache.drill.common.exceptions.UserException;

import com.univocity.parsers.common.TextParsingException;

/*******************************************************************************
 * Portions Copyright 2014 uniVocity Software Pty Ltd
 ******************************************************************************/

/**
 * A byte-based Text parser implementation. Builds heavily upon the uniVocity parsers. Customized for UTF8 parsing and
 * DrillBuf support.
 */
final class TextReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TextReader.class);

  private static final byte NULL_BYTE = (byte) '\0';

  private final TextParsingContext context;

  private final long recordsToRead;
  private final TextParsingSettings settings;

  private final TextInput input;
  private final TextOutput output;
  private final DrillBuf workBuf;

  private byte ch;

  // index of the field within this record
  private int fieldIndex;

  /** Behavior settings **/
  private final boolean ignoreTrailingWhitespace;
  private final boolean ignoreLeadingWhitespace;
  private final boolean parseUnescapedQuotes;

  /** Key Characters **/
  private final byte comment;
  private final byte delimiter;
  private final byte quote;
  private final byte quoteEscape;
  private final byte newLine;

  /**
   * The CsvParser supports all settings provided by {@link CsvParserSettings}, and requires this configuration to be
   * properly initialized.
   * @param settings  the parser configuration
   * @param input  input stream
   * @param output  interface to produce output record batch
   * @param workBuf  working buffer to handle whitespaces
   */
  public TextReader(TextParsingSettings settings, TextInput input, TextOutput output, DrillBuf workBuf) {
    this.context = new TextParsingContext(input, output);
    this.workBuf = workBuf;
    this.settings = settings;

    this.recordsToRead = settings.getNumberOfRecordsToRead() == -1 ? Long.MAX_VALUE : settings.getNumberOfRecordsToRead();

    this.ignoreTrailingWhitespace = settings.isIgnoreTrailingWhitespaces();
    this.ignoreLeadingWhitespace = settings.isIgnoreLeadingWhitespaces();
    this.parseUnescapedQuotes = settings.isParseUnescapedQuotes();
    this.delimiter = settings.getDelimiter();
    this.quote = settings.getQuote();
    this.quoteEscape = settings.getQuoteEscape();
    this.newLine = settings.getNormalizedNewLine();
    this.comment = settings.getComment();

    this.input = input;
    this.output = output;

  }

  public TextOutput getOutput(){
    return output;
  }

  /* Check if the given byte is a white space. As per the univocity text reader
   * any ASCII <= ' ' is considered a white space. However since byte in JAVA is signed
   * we have an additional check to make sure its not negative
   */
  static final boolean isWhite(byte b){
    return b <= ' ' && b > -1;
  }

  // Inform the output interface to indicate we are starting a new record batch
  public void resetForNextBatch(){
    output.startBatch();
  }

  public long getPos(){
    return input.getPos();
  }

  /**
   * Function encapsulates parsing an entire record, delegates parsing of the
   * fields to parseField() function.
   * We mark the start of the record and if there are any failures encountered (OOM for eg)
   * then we reset the input stream to the marked position
   * @return  true if parsing this record was successful; false otherwise
   * @throws IOException
   */
  private boolean parseRecord() throws IOException {
    final byte newLine = this.newLine;
    final TextInput input = this.input;

    input.mark();

    fieldIndex = 0;
    if (isWhite(ch) && ignoreLeadingWhitespace) {
      skipWhitespace();
    }

    int fieldsWritten = 0;
    try{
      boolean earlyTerm = false;
      while (ch != newLine) {
        earlyTerm = !parseField();
        fieldsWritten++;
        if (ch != newLine) {
          ch = input.nextChar();
          if (ch == newLine) {
            output.startField(fieldsWritten++);
            output.endEmptyField();
            break;
          }
        }
        if(earlyTerm){
          if(ch != newLine){
            input.skipLines(1);
          }
          break;
        }
      }
    }catch(StreamFinishedPseudoException e){
      // if we've written part of a field or all of a field, we should send this row.
      if(fieldsWritten == 0 && !output.rowHasData()){
        throw e;
      }
    }

    output.finishRecord();
    return true;
  }

  /**
   * Function parses an individual field and ignores any white spaces encountered
   * by not appending it to the output vector
   * @throws IOException
   */
  private void parseValueIgnore() throws IOException {
    final byte newLine = this.newLine;
    final byte delimiter = this.delimiter;
    final TextOutput output = this.output;
    final TextInput input = this.input;

    byte ch = this.ch;
    while (ch != delimiter && ch != newLine) {
      output.appendIgnoringWhitespace(ch);
//      fieldSize++;
      ch = input.nextChar();
    }
    this.ch = ch;
  }

  /**
   * Function parses an individual field and appends all characters till the delimeter (or newline)
   * to the output, including white spaces
   * @throws IOException
   */
  private void parseValueAll() throws IOException {
    final byte newLine = this.newLine;
    final byte delimiter = this.delimiter;
    final TextOutput output = this.output;
    final TextInput input = this.input;

    byte ch = this.ch;
    while (ch != delimiter && ch != newLine) {
      output.append(ch);
      ch = input.nextChar();
    }
    this.ch = ch;
  }

  /**
   * Function simply delegates the parsing of a single field to the actual implementation based on parsing config
   * @throws IOException
   */
  private void parseValue() throws IOException {
    if (ignoreTrailingWhitespace) {
      parseValueIgnore();
    }else{
      parseValueAll();
    }
  }

  /**
   * Recursive function invoked when a quote is encountered. Function also
   * handles the case when there are non-white space characters in the field
   * after the quoted value.
   * @param prev  previous byte read
   * @throws IOException
   */
  private void parseQuotedValue(byte prev) throws IOException {
    final byte newLine = this.newLine;
    final byte delimiter = this.delimiter;
    final TextOutput output = this.output;
    final TextInput input = this.input;
    final byte quote = this.quote;

    ch = input.nextCharNoNewLineCheck();

    while (!(prev == quote && (ch == delimiter || ch == newLine || isWhite(ch)))) {
      if (ch != quote) {
        if (prev == quote) { // unescaped quote detected
          if (parseUnescapedQuotes) {
            output.append(quote);
            output.append(ch);
            parseQuotedValue(ch);
            break;
          } else {
            throw new TextParsingException(
                context,
                "Unescaped quote character '"
                    + quote
                    + "' inside quoted value of CSV field. To allow unescaped quotes, set 'parseUnescapedQuotes' to 'true' in the CSV parser settings. Cannot parse CSV input.");
          }
        }
        output.append(ch);
        prev = ch;
      } else if (prev == quoteEscape) {
        output.append(quote);
        prev = NULL_BYTE;
      } else {
        prev = ch;
      }
      ch = input.nextCharNoNewLineCheck();
    }

    // Handles whitespaces after quoted value:
    // Whitespaces are ignored (i.e., ch <= ' ') if they are not used as delimiters (i.e., ch != ' ')
    // For example, in tab-separated files (TSV files), '\t' is used as delimiter and should not be ignored
    // Content after whitespaces may be parsed if 'parseUnescapedQuotes' is enabled.
    if (ch != newLine && ch <= ' ' && ch != delimiter) {
      final DrillBuf workBuf = this.workBuf;
      workBuf.resetWriterIndex();
      do {
        // saves whitespaces after value
        workBuf.writeByte(ch);
        ch = input.nextChar();
        // found a new line, go to next record.
        if (ch == newLine) {
          return;
        }
      } while (ch <= ' ' && ch != delimiter);

      // there's more stuff after the quoted value, not only empty spaces.
      if (!(ch == delimiter || ch == newLine) && parseUnescapedQuotes) {

        output.append(quote);
        for(int i =0; i < workBuf.writerIndex(); i++){
          output.append(workBuf.getByte(i));
        }
        // the next character is not the escape character, put it there
        if (ch != quoteEscape) {
          output.append(ch);
        }
        // sets this character as the previous character (may be escaping)
        // calls recursively to keep parsing potentially quoted content
        parseQuotedValue(ch);
      }
    }

    if (!(ch == delimiter || ch == newLine)) {
      throw new TextParsingException(context, "Unexpected character '" + ch
          + "' following quoted value of CSV field. Expecting '" + delimiter + "'. Cannot parse CSV input.");
    }
  }

  /**
   * Captures the entirety of parsing a single field and based on the input delegates to the appropriate function
   * @return
   * @throws IOException
   */
  private final boolean parseField() throws IOException {

    output.startField(fieldIndex++);

    if (isWhite(ch) && ignoreLeadingWhitespace) {
      skipWhitespace();
    }

    if (ch == delimiter) {
      return output.endEmptyField();
    } else {
      if (ch == quote) {
        parseQuotedValue(NULL_BYTE);
      } else {
        parseValue();
      }

      return output.endField();
    }

  }

  /**
   * Helper function to skip white spaces occurring at the current input stream.
   * @throws IOException
   */
  private void skipWhitespace() throws IOException {
    final byte delimiter = this.delimiter;
    final byte newLine = this.newLine;
    final TextInput input = this.input;

    while (isWhite(ch) && ch != delimiter && ch != newLine) {
      ch = input.nextChar();
    }
  }

  /**
   * Starting point for the reader. Sets up the input interface.
   * @throws IOException
   */
  public final void start() throws IOException {
    context.stopped = false;
    input.start();
  }


  /**
   * Parses the next record from the input. Will skip the line if its a comment,
   * this is required when the file contains headers
   * @throws IOException
   */
  public final boolean parseNext() throws IOException {
    try {
      while (!context.stopped) {
        ch = input.nextChar();
        if (ch == comment) {
          input.skipLines(1);
          continue;
        }
        break;
      }
      final long initialLineNumber = input.lineCount();
      boolean success = parseRecord();
      if (initialLineNumber + 1 < input.lineCount()) {
        throw new TextParsingException(context, "Cannot use newline character within quoted string");
      }

      if (success) {
        if (recordsToRead > 0 && context.currentRecord() >= recordsToRead) {
          context.stop();
        }
        return true;
      } else {
        return false;
      }

    } catch (UserException ex) {
      stopParsing();
      throw ex;
    } catch (StreamFinishedPseudoException ex) {
      stopParsing();
      return false;
    } catch (Exception ex) {
      try {
        throw handleException(ex);
      } finally {
        stopParsing();
      }
    }
  }

  private void stopParsing(){

  }

  private String displayLineSeparators(String str, boolean addNewLine) {
    if (addNewLine) {
      if (str.contains("\r\n")) {
        str = str.replaceAll("\\r\\n", "[\\\\r\\\\n]\r\n\t");
      } else if (str.contains("\n")) {
        str = str.replaceAll("\\n", "[\\\\n]\n\t");
      } else {
        str = str.replaceAll("\\r", "[\\\\r]\r\t");
      }
    } else {
      str = str.replaceAll("\\n", "\\\\n");
      str = str.replaceAll("\\r", "\\\\r");
    }
    return str;
  }

  /**
   * Helper method to handle exceptions caught while processing text files and generate better error messages associated with
   * the exception.
   * @param ex  Exception raised
   * @return
   * @throws IOException
   */
  private TextParsingException handleException(Exception ex) throws IOException {

    if (ex instanceof TextParsingException) {
      throw (TextParsingException) ex;
    }

    if (ex instanceof ArrayIndexOutOfBoundsException) {
      ex = UserException
          .dataReadError(ex)
          .message(
              "Drill failed to read your text file.  Drill supports up to %d columns in a text file.  Your file appears to have more than that.",
              RepeatedVarCharOutput.MAXIMUM_NUMBER_COLUMNS)
          .build(logger);
    }

    String message = null;
    String tmp = input.getStringSinceMarkForError();
    char[] chars = tmp.toCharArray();
    if (chars != null) {
      int length = chars.length;
      if (length > settings.getMaxCharsPerColumn()) {
        message = "Length of parsed input (" + length
            + ") exceeds the maximum number of characters defined in your parser settings ("
            + settings.getMaxCharsPerColumn() + "). ";
      }

      if (tmp.contains("\n") || tmp.contains("\r")) {
        tmp = displayLineSeparators(tmp, true);
        String lineSeparator = displayLineSeparators(settings.getLineSeparatorString(), false);
        message += "\nIdentified line separator characters in the parsed content. This may be the cause of the error. The line separator in your parser settings is set to '"
            + lineSeparator + "'. Parsed content:\n\t" + tmp;
      }

      int nullCharacterCount = 0;
      // ensuring the StringBuilder won't grow over Integer.MAX_VALUE to avoid OutOfMemoryError
      int maxLength = length > Integer.MAX_VALUE / 2 ? Integer.MAX_VALUE / 2 - 1 : length;
      StringBuilder s = new StringBuilder(maxLength);
      for (int i = 0; i < maxLength; i++) {
        if (chars[i] == '\0') {
          s.append('\\');
          s.append('0');
          nullCharacterCount++;
        } else {
          s.append(chars[i]);
        }
      }
      tmp = s.toString();

      if (nullCharacterCount > 0) {
        message += "\nIdentified "
            + nullCharacterCount
            + " null characters ('\0') on parsed content. This may indicate the data is corrupt or its encoding is invalid. Parsed content:\n\t"
            + tmp;
      }

    }

    throw new TextParsingException(context, message, ex);
  }

  /**
   * Finish the processing of a batch, indicates to the output
   * interface to wrap up the batch
   */
  public void finishBatch(){
    output.finishBatch();
//    System.out.println(String.format("line %d, cnt %d", input.getLineCount(), output.getRecordCount()));
  }

  /**
   * Invoked once there are no more records and we are done with the
   * current record reader to clean up state.
   * @throws IOException
   */
  public void close() throws IOException{
    input.close();
  }

  @Override
  public String toString() {
    return "TextReader[Line=" + context.currentLine()
        + ", Column=" + context.currentChar()
        + ", Record=" + context.currentRecord()
        + ", Byte pos=" + getPos()
        + "]";
  }
}
