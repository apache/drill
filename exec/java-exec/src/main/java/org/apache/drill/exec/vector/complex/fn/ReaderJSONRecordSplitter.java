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
package org.apache.drill.exec.vector.complex.fn;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

public class ReaderJSONRecordSplitter extends JsonRecordSplitterBase {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReaderJSONRecordSplitter.class);

  private Reader reader;

  public ReaderJSONRecordSplitter(String str){
    this(new StringReader(str));
  }

  public ReaderJSONRecordSplitter(Reader reader){
    this.reader = reader;
  }

  @Override
  protected void preScan() throws IOException {
    reader.mark(MAX_RECORD_SIZE);
  }

  @Override
  protected void postScan() throws IOException {
    reader.reset();
  }

  @Override
  protected int readNext() throws IOException {
    return reader.read();
  }

  @Override
  protected Reader createReader(long maxBytes) {
    return new LimitedReader(reader, (int)maxBytes);
  }

  private class LimitedReader extends Reader {

    private final Reader incoming;
    private final int maxBytes;
    private int markedBytes = 0;
    private int bytes = 0;

    public LimitedReader(Reader in, int maxBytes) {
      this.maxBytes = maxBytes;
      this.incoming = in;
    }

    @Override
    public int read() throws IOException {
      if (bytes >= maxBytes){
        return -1;
      }else{
        bytes++;
        return incoming.read();
      }
    }

    @Override
    public void mark(int readAheadLimit) throws IOException {
      incoming.mark(readAheadLimit);
      markedBytes = bytes;
    }

    @Override
    public void reset() throws IOException {
      incoming.reset();
      bytes = markedBytes;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
      int outputLength = Math.min(len, maxBytes - bytes);
      if(outputLength > 0){
        incoming.read(cbuf, off, outputLength);
        bytes += outputLength;
        return outputLength;
      }else{
        return -1;
      }
    }

    @Override
    public void close() throws IOException { }
  }
}
