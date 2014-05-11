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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;

public class UTF8JsonRecordSplitter implements JsonRecordSplitter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UTF8JsonRecordSplitter.class);

  private static final int OPEN_CBRACKET = '{';
  private static final int OPEN_BRACKET = '[';
  private static final int CLOSE_CBRACKET = '}';
  private static final int CLOSE_BRACKET = ']';

  private static final int SPACE = ' ';
  private static final int TAB = '\t';
  private static final int NEW_LINE = '\n';
  private static final int FORM_FEED = '\f';
  private static final int CR = '\r';

  private long start = 0;
  private InputStream incoming;

  public UTF8JsonRecordSplitter(InputStream incoming){
    this.incoming = new BufferedInputStream(incoming);
  }

  @Override
  public Reader getNextReader() throws IOException{

    boolean inCandidate = false;
    boolean found = false;

    incoming.mark(128*1024);
    long endOffset = start;
    outside: while(true){
      int b = incoming.read();
//      System.out.println(b);
      endOffset++;

      if(b == -1){
        if(inCandidate){
          found = true;
        }
        break;
      }

      switch(b){
      case CLOSE_BRACKET:
      case CLOSE_CBRACKET:
//        System.out.print("c");
        inCandidate = true;
        break;
      case OPEN_BRACKET:
      case OPEN_CBRACKET:
//        System.out.print("o");
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
//        System.out.print(' ');
        break;

      default:
//        System.out.print('-');
        inCandidate = false;
      }
    }

    if(found){
      long maxBytes = endOffset - 1 - start;
      start = endOffset;
      incoming.reset();
      return new BufferedReader(new InputStreamReader(new DelInputStream(incoming, maxBytes), Charsets.UTF_8));
    }else{
      return null;
    }

  }

  private class DelInputStream extends InputStream {

    private final InputStream incoming;
    private final long maxBytes;
    private long bytes = 0;

    public DelInputStream(InputStream in, long maxBytes) {
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

  }

  public static void main(String[] args) throws Exception{
    byte[] str = " { \"b\": \"hello\", \"c\": \"goodbye\", r: []}\n { \"b\": \"yellow\", \"c\": \"red\"}\n ".getBytes(Charsets.UTF_8);
    InputStream s = new ByteArrayInputStream(str);
    JsonRecordSplitter splitter = new UTF8JsonRecordSplitter(s);
    Reader obj = null;
    System.out.println();

    while( (obj = splitter.getNextReader()) != null){
      System.out.println();
      System.out.println(CharStreams.toString(obj));
      System.out.println("===end obj===");
    }
  }
}
