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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

public abstract class JacksonSerializable implements DrillSerializable{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JacksonSerializable.class);

  private void fail(){
    throw new UnsupportedOperationException("Need to register serialization config for class " + this.getClass().getName()); // rely on external serializer

  }

  @Override
  public void readData(ObjectDataInput input) throws IOException {
    fail();
  }

  @Override
  public void readFromStream(InputStream input) throws IOException {
    fail();
  }

  @Override
  public void writeData(ObjectDataOutput output) throws IOException {
    fail();
  }

  @Override
  public void writeToStream(OutputStream output) throws IOException {
    fail();
  }
}
