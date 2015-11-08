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
package org.apache.drill.exec.store.easy.text.compliant;

/*******************************************************************************
 * Copyright 2014 uniVocity Software Pty Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

import io.netty.buffer.DrillBuf;
import io.netty.util.internal.PlatformDependent;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.drill.exec.memory.BoundsChecking;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CompressionInputStream;

import com.google.common.base.Preconditions;
import com.univocity.parsers.common.Format;

/**
 * Class that fronts an InputStream to provide a byte consumption interface.
 * Also manages only reading lines to and from each split.
 */
final class TextInput {

  private static final byte NULL_BYTE = (byte) '\0';
  private final byte lineSeparator1;
  private final byte lineSeparator2;
  private final byte normalizedLineSeparator;
  private final TextParsingSettings settings;

  private long lineCount;
  private long charCount;

  /**
   * The starting position in the file.
   */
  private final long startPos;
  private final long endPos;

  private int bufferMark;
  private long streamMark;

  private long streamPos;

  private final Seekable seekable;
  private final FSDataInputStream inputFS;
  private final InputStream input;

  private final DrillBuf buffer;
  private final ByteBuffer underlyingBuffer;
  private final long bStart;
  private final long bStartMinus1;

  private final boolean bufferReadable;

  /**
   * Whether there was a possible partial line separator on the previous
   * read so we dropped it and it should be appended to next read.
   */
  private boolean remByte = false;

  /**
   * The current position in the buffer.
   */
  public int bufferPtr;

  /**
   * The quantity of valid data in the buffer.
   */
  public int length = -1;

  private boolean endFound = false;

  /**
   * Creates a new instance with the mandatory characters for handling newlines transparently.
   * @param lineSeparator the sequence of characters that represent a newline, as defined in {@link Format#getLineSeparator()}
   * @param normalizedLineSeparator the normalized newline character (as defined in {@link Format#getNormalizedNewline()}) that is used to replace any lineSeparator sequence found in the input.
   */
  public TextInput(TextParsingSettings settings, InputStream input, DrillBuf readBuffer, long startPos, long endPos) {
    byte[] lineSeparator = settings.getNewLineDelimiter();
    byte normalizedLineSeparator = settings.getNormalizedNewLine();
    Preconditions.checkArgument(lineSeparator != null && (lineSeparator.length == 1 || lineSeparator.length == 2), "Invalid line separator. Expected 1 to 2 characters");
    Preconditions.checkArgument(input instanceof Seekable, "Text input only supports an InputStream that supports Seekable.");
    boolean isCompressed = input instanceof CompressionInputStream ;
    Preconditions.checkArgument(!isCompressed || startPos == 0, "Cannot use split on compressed stream.");

    // splits aren't allowed with compressed data.  The split length will be the compressed size which means we'll normally end prematurely.
    if(isCompressed && endPos > 0){
      endPos = Long.MAX_VALUE;
    }

    this.input = input;
    this.seekable = (Seekable) input;
    this.settings = settings;

    if(input instanceof FSDataInputStream){
      this.inputFS = (FSDataInputStream) input;
      this.bufferReadable = inputFS.getWrappedStream() instanceof ByteBufferReadable;
    }else{
      this.inputFS = null;
      this.bufferReadable = false;
    }

    this.startPos = startPos;
    this.endPos = endPos;

    this.lineSeparator1 = lineSeparator[0];
    this.lineSeparator2 = lineSeparator.length == 2 ? lineSeparator[1] : NULL_BYTE;
    this.normalizedLineSeparator = normalizedLineSeparator;

    this.buffer = readBuffer;
    this.bStart = buffer.memoryAddress();
    this.bStartMinus1 = bStart -1;
    this.underlyingBuffer = buffer.nioBuffer(0, buffer.capacity());
  }

  /**
   * Test the input to position for read start.  If the input is a non-zero split or
   * splitFirstLine is enabled, input will move to appropriate complete line.
   * @throws IOException
   */
  final void start() throws IOException {
    lineCount = 0;
    if(startPos > 0){
      seekable.seek(startPos);
    }

    updateBuffer();
    if (length > 0) {
      if(startPos > 0 || settings.isSkipFirstLine()){

        // move to next full record.
        skipLines(1);
      }
    }
  }


  /**
   * Helper method to get the most recent characters consumed since the last record started.
   * May get an incomplete string since we don't support stream rewind.  Returns empty string for now.
   * @return String of last few bytes.
   * @throws IOException
   */
  public String getStringSinceMarkForError() throws IOException {
    return " ";
  }

  long getPos(){
    return streamPos + bufferPtr;
  }

  public void mark(){
    streamMark = streamPos;
    bufferMark = bufferPtr;
  }

  /**
   * read some more bytes from the stream.  Uses the zero copy interface if available.  Otherwise, does byte copy.
   * @throws IOException
   */
  private final void read() throws IOException {
    if(bufferReadable){

      if(remByte){
        underlyingBuffer.put(lineSeparator1);
        remByte = false;
      }
      length = inputFS.read(underlyingBuffer);

    }else{

      byte[] b = new byte[underlyingBuffer.capacity()];
      if(remByte){
        b[0] = lineSeparator1;
        length = input.read(b, 1, b.length - 1);
        remByte = false;
      }else{
        length = input.read(b);
      }

      underlyingBuffer.put(b);
    }
  }


  /**
   * Read more data into the buffer.  Will also manage split end conditions.
   * @throws IOException
   */
  private final void updateBuffer() throws IOException {
    streamPos = seekable.getPos();
    underlyingBuffer.clear();

    if(endFound){
      length = -1;
      return;
    }

    read();

    // check our data read allowance.
    if(streamPos + length >= this.endPos){
      updateLengthBasedOnConstraint();
    }

    charCount += bufferPtr;
    bufferPtr = 1;

    buffer.writerIndex(underlyingBuffer.limit());
    buffer.readerIndex(underlyingBuffer.position());

  }

  /**
   * Checks to see if we can go over the end of our bytes constraint on the data.  If so,
   * adjusts so that we can only read to the last character of the first line that crosses
   * the split boundary.
   */
  private void updateLengthBasedOnConstraint(){
    // we've run over our alotted data.
    final byte lineSeparator1 = this.lineSeparator1;
    final byte lineSeparator2 = this.lineSeparator2;

    // find the next line separator:
    final long max = bStart + length;

    for(long m = this.bStart + (endPos - streamPos); m < max; m++){
      if(PlatformDependent.getByte(m) == lineSeparator1){
        // we found a potential line break.

        if(lineSeparator2 == NULL_BYTE){
          // we found a line separator and don't need to consult the next byte.
          length = (int)(m - bStart);
          endFound = true;
          break;
        }else{
          // this is a two byte line separator.

          long mPlus = m+1;
          if(mPlus < max){
            // we can check next byte and see if the second lineSeparator is correct.
            if(lineSeparator2 == PlatformDependent.getByte(mPlus)){
              length = (int)(mPlus - bStart);
              endFound = true;
              break;
            }else{
              // this was a partial line break.
              continue;
            }
          }else{
            // the last character of the read was a remnant byte.  We'll hold off on dealing with this byte until the next read.
            remByte = true;
            length -= 1;
            break;
          }

        }
      }
    }
  }

  /**
   * Get next byte from stream.  Also maintains the current line count.  Will throw a StreamFinishedPseudoException
   * when the stream has run out of bytes.
   * @return next byte from stream.
   * @throws IOException
   */
  public final byte nextChar() throws IOException {
    final byte lineSeparator1 = this.lineSeparator1;
    final byte lineSeparator2 = this.lineSeparator2;

    if (length == -1) {
      throw StreamFinishedPseudoException.INSTANCE;
    }

    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      buffer.checkBytes(bufferPtr - 1, bufferPtr);
    }

    byte byteChar = PlatformDependent.getByte(bStartMinus1 + bufferPtr);

    if (bufferPtr >= length) {
      if (length != -1) {
        updateBuffer();
        bufferPtr--;
      } else {
        throw StreamFinishedPseudoException.INSTANCE;
      }
    }

    bufferPtr++;

    // monitor for next line.
    if (lineSeparator1 == byteChar && (lineSeparator2 == NULL_BYTE || lineSeparator2 == buffer.getByte(bufferPtr - 1))) {
      lineCount++;

      if (lineSeparator2 != NULL_BYTE) {
        byteChar = normalizedLineSeparator;

        if (bufferPtr >= length) {
          if (length != -1) {
            updateBuffer();
          } else {
            throw StreamFinishedPseudoException.INSTANCE;
          }
        }
      }
    }
    return byteChar;
  }

  /**
   * Number of lines read since the start of this split.
   * @return
   */
  public final long lineCount() {
    return lineCount;
  }

  /**
   * Skip forward the number of line delimiters.  If you are in the middle of a line,
   * a value of 1 will skip to the start of the next record.
   * @param lines Number of lines to skip.
   * @throws IOException
   */
  public final void skipLines(int lines) throws IOException {
    if (lines < 1) {
      return;
    }
    long expectedLineCount = this.lineCount + lines;

    try {
      do {
        nextChar();
      } while (lineCount < expectedLineCount);
      if (lineCount < lines) {
        throw new IllegalArgumentException("Unable to skip " + lines + " lines from line " + (expectedLineCount - lines) + ". End of input reached");
      }
    } catch (EOFException ex) {
      throw new IllegalArgumentException("Unable to skip " + lines + " lines from line " + (expectedLineCount - lines) + ". End of input reached");
    }
  }

  public final long charCount() {
    return charCount + bufferPtr;
  }

  public long getLineCount() {
    return lineCount;
  }

  public void close() throws IOException{
    input.close();
  }
}
