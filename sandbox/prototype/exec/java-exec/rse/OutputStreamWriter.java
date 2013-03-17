/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.ref.rse;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.RunOutcome.OutcomeType;
import org.apache.drill.exec.ref.rops.DataWriter;
import org.apache.drill.exec.ref.rops.DataWriter.ConverterType;
import org.apache.hadoop.fs.FSDataOutputStream;

public class OutputStreamWriter implements RecordRecorder{
  
  private OutputStream stream;
  private FSDataOutputStream posStream;
  private DataWriter writer;
  private ConverterType type;
  private boolean closeStream;
  
  public OutputStreamWriter(OutputStream stream, ConverterType type, boolean closeStream){
    this.stream = stream;
    this.closeStream = closeStream;
    this.type = type;
    if(stream instanceof FSDataOutputStream) posStream = (FSDataOutputStream) stream;
  }

  @Override
  public void setup() throws IOException {
    DataWriter w = null;
    switch(type){
    case JSON:
      w = new JSONDataWriter(stream);
      break;
    default:
      throw new UnsupportedOperationException();
    }
    this.writer = w;
  }
  
  private long getPos() throws IOException{
    if(posStream == null) return 0;
    return posStream.getPos();
  }

  @Override
  public long recordRecord(RecordPointer pointer) throws IOException {
    pointer.write(writer);
    return getPos();
  }

  @Override
  public void finish(OutcomeType outcome) throws IOException {
    writer.finish();
    if(closeStream){
      stream.close();
    }else{
      stream.flush();
    }
  }
  
}
