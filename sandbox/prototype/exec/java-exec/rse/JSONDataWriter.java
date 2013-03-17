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

import org.apache.drill.exec.ref.rops.DataWriter;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

public class JSONDataWriter implements DataWriter{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JSONDataWriter.class);
  
  private final JsonGenerator g;
//  private CharSequence transientName;
  
  public JSONDataWriter(OutputStream out) throws IOException{
    JsonFactory f = new JsonFactory();
    
    this.g = f.createJsonGenerator(out, JsonEncoding.UTF8);
    this.g.useDefaultPrettyPrinter();
  }
  
  private String s(CharSequence seq) {
    String s = (seq instanceof String) ? (String) seq : seq.toString();
    return s;
  }
  
  @Override
  public void startRecord() throws IOException {
    
  }

  @Override
  public void writeArrayStart(int length) throws IOException {
    g.writeStartArray();
  }

  @Override
  public void writeArrayElementStart() throws IOException {
  }

  @Override
  public void writeArrayElementEnd() throws IOException {
  }

  @Override
  public void writeArrayEnd() throws IOException {
    g.writeEndArray();
  }

  @Override
  public void writeMapStart() throws IOException {
    g.writeStartObject();
  }

  @Override
  public void writeMapKey(CharSequence seq) throws IOException {
    g.writeFieldName(s(seq));
  }

  @Override
  public void writeMapValueStart() throws IOException {
  }

  @Override
  public void writeMapValueEnd() throws IOException {
  }

  @Override
  public void writeMapEnd() throws IOException {
    g.writeEndObject();
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {
    g.writeBoolean(b);
  }

  @Override
  public void writeSInt32(int value) throws IOException {
    g.writeNumber(value);
  }

  @Override
  public void writeSInt64(long value) throws IOException {
    g.writeNumber(value);
  }

  @Override
  public void writeBytes(byte[] bytes) throws IOException {
    g.writeBinary(bytes);
  }

  @Override
  public void writeSFloat64(double value) throws IOException {
    g.writeNumber(value);
  }

  @Override
  public void writeSFloat32(float value) throws IOException {
    g.writeNumber(value);
  }

  @Override
  public void writeNullValue() throws IOException {
    g.writeNull();
  }

  @Override
  public void writeCharSequence(CharSequence value) throws IOException {
    g.writeString(s(value));
  }

  @Override
  public void endRecord() throws IOException {
    g.writeRawValue("\n");
  }
  
  public void finish() throws IOException{
    g.close();
  }
  
}
