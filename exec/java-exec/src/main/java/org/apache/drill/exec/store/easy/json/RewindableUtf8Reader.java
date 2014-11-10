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
package org.apache.drill.exec.store.easy.json;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.Seekable;

import com.fasterxml.jackson.core.JsonStreamContextExposer;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.json.JsonReadContext;
import com.fasterxml.jackson.core.json.JsonReadContextExposer;
import com.fasterxml.jackson.core.json.UTF8StreamJsonParser;
import com.fasterxml.jackson.core.sym.BytesToNameCanonicalizer;

/**
 * An extended version of Jaskon's UTF8StreamJsonParser that supports rewind the stream to the previous record.
 */
public class RewindableUtf8Reader<T extends InputStream & Seekable> extends UTF8StreamJsonParser {

  private T in;

  /**
   * Index of character after last available one in the buffer.
   */
  private long markFilePos;
  private int markInputPtr;
  private int markInputEnd;
  private long markInputProcessed;
  private int markInputRow;
  private int markInputRowStart;
  private long markInputTotal;
  private int markTokenInputRow;
  private int markTokenInputCol;
  private JsonToken markToken;
  private JsonToken markLastToken;
  private JsonReadContext markContext;
  private JsonReadContext rootContext;

  private int type;
  private int lineNr;
  private int colNr;

  private boolean closed = false;

  public RewindableUtf8Reader(IOContext ctxt, int features, BytesToNameCanonicalizer sym, byte[] inputBuffer) {
    super(ctxt, features, null, null, sym, inputBuffer, 0, 0, true);
    this.rootContext = this._parsingContext;
  }

  public void mark() throws IOException{
    this.markFilePos = this.in.getPos();
    this.markInputPtr = this._inputPtr;
    this.markInputEnd = this._inputEnd;
    this.markInputProcessed = this._currInputProcessed;
    this.markInputRow = this._currInputRow;
    this.markInputRowStart = this._currInputRowStart;
    this.markInputTotal = this._tokenInputTotal;
    this.markTokenInputCol = this._tokenInputCol;
    this.markTokenInputRow = this._tokenInputRow;
    this.markToken = this._currToken;
    this.markLastToken = this._lastClearedToken;
    this.markContext = this._parsingContext;
    this.type = JsonStreamContextExposer.getType(markContext);
    this.lineNr = JsonReadContextExposer.getLineNmbr(markContext);
    this.colNr = JsonReadContextExposer.getColNmbr(markContext);
  }

  public void resetToMark() throws IOException{
    if(markFilePos != in.getPos()){
      in.seek(markFilePos - _inputBuffer.length);
      in.read(_inputBuffer, 0, _inputBuffer.length);
    }
    this._inputPtr = this.markInputPtr;
    this._inputEnd = this.markInputEnd;
    this._currInputProcessed = this.markInputProcessed;
    this._currInputRow = this.markInputRow;
    this._currInputRowStart = this.markInputRowStart;
    this._tokenInputTotal = this.markInputTotal;
    this._tokenInputCol = this.markTokenInputCol;
    this._tokenInputRow = this.markTokenInputRow;
    this._currToken = this.markToken;
    this._lastClearedToken = this.markLastToken;
    this._parsingContext = this.markContext;
    JsonReadContextExposer.reset(markContext, type, lineNr, colNr);

  }

  @Override
  protected void _closeInput() throws IOException {
    super._closeInput();

      if (_inputStream != null) {
          if (_ioContext.isResourceManaged() || isEnabled(Feature.AUTO_CLOSE_SOURCE)) {
              _inputStream.close();
          }
          _inputStream = null;
      }
      this.closed = true;

  }

  public void setInputStream(T in) throws IOException{
    if(this.in != null){
      in.close();
    }

    this._inputStream = in;
    this.in = in;
    this._parsingContext = rootContext;
    this._inputPtr = 0;
    this._inputEnd = 0;
    this._currInputProcessed = 0;
    this._currInputRow = 0;
    this._currInputRowStart = 0;
    this._tokenInputTotal = 0;
    this._tokenInputCol = 0;
    this._tokenInputRow = 0;
    this._currToken = null;
    this._lastClearedToken = null;
    this.closed = false;
  }

  public boolean hasDataAvailable() throws IOException{
    return !closed;
  }

  @Override
  public String toString() {
    return "RewindableUtf8Reader [markFilePos=" + markFilePos + ", markInputPtr=" + markInputPtr + ", markInputEnd="
        + markInputEnd + ", markInputProcessed=" + markInputProcessed + ", markInputRow=" + markInputRow
        + ", markInputRowStart=" + markInputRowStart + ", markInputTotal=" + markInputTotal + ", markTokenInputRow="
        + markTokenInputRow + ", markTokenInputCol=" + markTokenInputCol + ", markToken=" + markToken
        + ", markLastToken=" + markLastToken + ", markContext=" + markContext + ", rootContext=" + rootContext
        + ", type=" + type + ", lineNr=" + lineNr + ", colNr=" + colNr + "]";
  }


}
