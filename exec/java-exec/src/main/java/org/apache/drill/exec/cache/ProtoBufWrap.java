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
package org.apache.drill.exec.cache;

import java.io.IOException;

import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

public abstract class ProtoBufWrap<T extends MessageLite> implements DataSerializable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProtoBufWrap.class);
  
  T value;
  final Parser<T> parser;
  
  public ProtoBufWrap(Parser<T> parser){
    this(null, parser);
  }
  
  public ProtoBufWrap(T value, Parser<T> parser){
    this.value = value;
    this.parser = parser;
  }
  
  @Override
  public void readData(ObjectDataInput arg0) throws IOException {
    int len = arg0.readShort();
    byte[] b = new byte[len];
    arg0.readFully(b);
    this.value = parser.parseFrom(b);
  }

  @Override
  public void writeData(ObjectDataOutput arg0) throws IOException {
    byte[] b = value.toByteArray();
    if (b.length > Short.MAX_VALUE) throw new IOException("Unexpectedly long value.");
    arg0.writeShort(b.length);
    arg0.write(b);
  }

  protected T get() {
    return value;
  }

  protected void set(T value) {
    this.value = value;
  }

}
