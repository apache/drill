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

import org.apache.drill.common.exceptions.DrillRuntimeException;

public abstract class JsonRecordSplitterBase implements JsonRecordSplitter {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReaderJSONRecordSplitter.class);
  public final static int MAX_RECORD_SIZE = JsonReader.MAX_RECORD_SIZE;

  private static final int OPEN_CBRACKET = '{';
  private static final int OPEN_BRACKET = '[';
  private static final int CLOSE_CBRACKET = '}';
  private static final int CLOSE_BRACKET = ']';
  private static final int ESCAPE = '\\';
  private static final int LITERAL = '"';

  private static final int SPACE = ' ';
  private static final int TAB = '\t';
  private static final int NEW_LINE = '\n';
  private static final int FORM_FEED = '\f';
  private static final int CR = '\r';

  private long start = 0;


  protected void preScan() throws IOException { }

  protected void postScan() throws IOException { }

  protected abstract int readNext() throws IOException;

  protected abstract Reader createReader(long maxBytes);

  @Override
  public Reader getNextReader() throws IOException {
    preScan();

    boolean isEscaped = false;
    boolean inLiteral = false;
    boolean inCandidate = false;
    boolean found = false;

    long endOffset = start;
    long curBytes = 0;
    int cur;
    outside: while(true) {
      cur = readNext();
      endOffset++;
      curBytes = endOffset - 1 - start;
      if (curBytes > MAX_RECORD_SIZE) {
        throw new DrillRuntimeException(String.format("Record is too long. Max allowed record size is %s bytes.", MAX_RECORD_SIZE));
      }

      if(cur == -1) {
        if(inCandidate){
          found = true;
        }
        break;
      }

      switch(cur) {
        case ESCAPE:
          isEscaped = !isEscaped;
          break;
        case LITERAL:
          if (!isEscaped) {
            inLiteral = !inLiteral;
          }
          isEscaped = false;
          break;
        case CLOSE_BRACKET:
        case CLOSE_CBRACKET:
          inCandidate = !inLiteral;
          break;
        case OPEN_BRACKET:
        case OPEN_CBRACKET:
          if(inCandidate){
            found = true;
            break outside;
          }
          break;

        case SPACE:
        case TAB:
        case NEW_LINE:
        case CR:
        case FORM_FEED:
          break;

        default:
          inCandidate = false;
          isEscaped = false;
      }
    }

    if(!found) {
      return null;
    }

    postScan();
    start = endOffset;
    return createReader(curBytes);
  }
}
