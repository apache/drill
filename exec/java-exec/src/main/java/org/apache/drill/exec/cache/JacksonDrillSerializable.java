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

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.planner.logical.StoragePlugins;
import org.apache.drill.exec.server.DrillbitContext;

import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class JacksonDrillSerializable<T> extends LoopedAbstractDrillSerializable implements DrillSerializable{
  private ObjectMapper mapper;
  private T obj;
  private Class<T> clazz;

  public JacksonDrillSerializable(DrillbitContext context, T obj, Class<T> clazz) {
    this(clazz);
    this.mapper = context.getConfig().getMapper();
    this.obj = obj;
  }

  public JacksonDrillSerializable(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public void readFromStream(InputStream input) throws IOException {
    mapper = DrillConfig.create().getMapper();
    obj = (T) mapper.readValue(input, clazz);
  }

  @Override
  public void writeToStream(OutputStream output) throws IOException {
    output.write(mapper.writeValueAsBytes(obj));
  }

  public T getObj() {
    return obj;
  }

  public static class StoragePluginsSerializable extends JacksonDrillSerializable<StoragePlugins> {

    public StoragePluginsSerializable(DrillbitContext context, StoragePlugins obj) {
      super(context, obj, StoragePlugins.class);
    }

    public StoragePluginsSerializable(BufferAllocator allocator) {
      super(StoragePlugins.class);
    }

    public StoragePluginsSerializable() {
      super(StoragePlugins.class);
    }


  }
}
