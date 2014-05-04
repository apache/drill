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
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

public abstract class ProtobufDrillSerializable<T extends Message> extends LoopedAbstractDrillSerializable implements DrillSerializable{
  private Parser<T> parser;
  private T obj;

  public ProtobufDrillSerializable(T obj){
    this.parser = (Parser<T>) obj.getParserForType();
    this.obj = obj;
  }

  public ProtobufDrillSerializable(Parser<T> parser) {
    this.parser = parser;
  }

  @Override
  public void readFromStream(InputStream input) throws IOException {
    obj = parser.parseDelimitedFrom(input);
  }

  @Override
  public void writeToStream(OutputStream output) throws IOException {
    obj.writeDelimitedTo(output);
  }

  public T getObj() {
    return obj;
  }

  public static class CQueryProfile extends ProtobufDrillSerializable<QueryProfile>{

    public CQueryProfile(BufferAllocator allocator){
      super(QueryProfile.PARSER);
    }

    public CQueryProfile() {
      super(QueryProfile.PARSER);

    }

    public CQueryProfile(QueryProfile obj) {
      super(obj);
    }

  }
}
