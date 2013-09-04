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
package org.apache.drill.exec.ref.rops;

import java.io.IOException;

public interface DataWriter {
  public void startRecord() throws IOException;
  public void writeArrayStart(int length) throws IOException;
  public void writeArrayElementStart() throws IOException;
  public void writeArrayElementEnd() throws IOException;
  public void writeArrayEnd() throws IOException;
  
  public void writeMapStart() throws IOException;
  public void writeMapKey(CharSequence seq) throws IOException;
  public void writeMapValueStart() throws IOException;
  public void writeMapValueEnd() throws IOException;
  public void writeMapEnd() throws IOException;
  
  public void writeBoolean(boolean b) throws IOException;
  public void writeSInt32(int value) throws IOException;
  public void writeSInt64(long value) throws IOException;
  public void writeBytes(byte[] bytes) throws IOException;
  public void writeSFloat64(double value) throws IOException;
  public void writeSFloat32(float value) throws IOException;
  public void writeNullValue() throws IOException;
  public void writeCharSequence(CharSequence value) throws IOException;
  public void endRecord() throws IOException;
  public void finish() throws IOException;
  
  public enum ConverterType{
    JSON
  }
}
