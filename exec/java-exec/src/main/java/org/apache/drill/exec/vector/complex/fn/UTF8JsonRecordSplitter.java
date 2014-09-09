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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;

public class UTF8JsonRecordSplitter extends JsonRecordSplitterBase {
  private final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UTF8JsonRecordSplitter.class);

  private final InputStream incoming;

  public UTF8JsonRecordSplitter(InputStream incoming){
    this.incoming = new BufferedInputStream(incoming);
  }

  @Override
  protected void preScan() throws IOException {
    incoming.mark(MAX_RECORD_SIZE);
  }

  @Override
  protected void postScan() throws IOException {
    incoming.reset();
  }

  @Override
  protected int readNext() throws IOException {
    return incoming.read();
  }

  @Override
  protected Reader createReader(long maxBytes) {
    return new BufferedReader(new InputStreamReader(new DelInputStream(incoming, maxBytes), Charsets.UTF_8));
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

  public static void main(String[] args) throws Exception {
    String path = "/Users/hgunes/workspaces/mapr/incubator-drill/yelp_academic_dataset_review.json";
    InputStream s = new FileInputStream(new File(path));
    JsonRecordSplitter splitter = new UTF8JsonRecordSplitter(s);
    int recordCount = 0;
    Reader record = null;
    ObjectMapper mapper = new ObjectMapper();
    try {
      while ((record = splitter.getNextReader()) != null) {
        recordCount++;
        JsonNode node = mapper.readTree(record);
        out(node.toString());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    out("last record: " + recordCount);
  }

  static void out(Object thing) {
    System.out.println(thing);
  }
}
