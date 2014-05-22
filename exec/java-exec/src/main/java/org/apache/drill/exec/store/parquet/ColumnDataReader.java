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
package org.apache.drill.exec.store.parquet;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.bytes.BytesInput;
import parquet.format.PageHeader;
import parquet.format.Util;

class ColumnDataReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ColumnDataReader.class);
  
  private final long endPosition;
  public final FSDataInputStream input;
  // 1 mb, the page size can range from 8kb to 50Mb, the buffer is re-allocated only if it hits a larger page
  private static final int DEFUALT_PAGE_BUFFER_SIZE = 1024 * 1024 * 5;
  private static final float BUFFER_GROWTH_RATE = 1.25f;
  private byte[] buffer;
  
  public ColumnDataReader(FSDataInputStream input, long start, long length) throws IOException{
    this.input = input;
    this.input.seek(start);
    this.endPosition = start + length;
    this.buffer = new byte[DEFUALT_PAGE_BUFFER_SIZE];
  }
  
  public PageHeader readPageHeader() throws IOException{
    try{
    return Util.readPageHeader(input);
    }catch (IOException e) {
      throw e;
    }
  }

  static int allocCount = 0;
  public BytesInput getPageAsBytesInput(int pageLength) throws IOException {
    if (buffer.length < pageLength) {
      buffer = new byte[(int) (BUFFER_GROWTH_RATE * pageLength)];
      allocCount++;
      System.out.println(allocCount);
    }
    input.read(buffer, 0, pageLength);
    return new HadoopBytesInput(buffer, pageLength);
  }
  
  public void clear(){
    try{
      input.close();
    }catch(IOException ex){
      logger.warn("Error while closing input stream.", ex);
    }
  }

  public boolean hasRemainder() throws IOException{
    return input.getPos() < endPosition;
  }
  
  public class HadoopBytesInput extends BytesInput{

    private final byte[] pageBytes;
    private final int lengthUsed;
    
    public HadoopBytesInput(byte[] pageBytes, int lengthUsed) {
      super();
      this.lengthUsed = lengthUsed;
      this.pageBytes = pageBytes;
    }

    @Override
    public byte[] toByteArray() throws IOException {
      return pageBytes;
    }

    @Override
    public long size() {
      return lengthUsed;
    }

    @Override
    public void writeAllTo(OutputStream out) throws IOException {
      out.write(pageBytes, 0, lengthUsed);
    }
    
  }
}
