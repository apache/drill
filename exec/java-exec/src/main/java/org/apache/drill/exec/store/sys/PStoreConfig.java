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
package org.apache.drill.exec.store.sys;

import org.apache.drill.exec.store.sys.serialize.JacksonSerializer;
import org.apache.drill.exec.store.sys.serialize.PClassSerializer;
import org.apache.drill.exec.store.sys.serialize.ProtoSerializer;

import com.dyuproject.protostuff.Schema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

public class PStoreConfig<V> {

  private final String name;
  private final PClassSerializer<V> valueSerializer;

  private PStoreConfig(String name, PClassSerializer<V> valueSerializer) {
    super();
    this.name = name;
    this.valueSerializer = valueSerializer;
  }

  public String getName() {
    return name;
  }

  public PClassSerializer<V> getSerializer() {
    return valueSerializer;
  }

  public static <V extends Message, X extends Builder> PStoreConfigBuilder<V> newProtoBuilder(Schema<V> writeSchema, Schema<X> readSchema) {
    return new PStoreConfigBuilder<V>(new ProtoSerializer<V, X>(writeSchema, readSchema));
  }

  public static <V> PStoreConfigBuilder<V> newJacksonBuilder(ObjectMapper mapper, Class<V> clazz) {
    return new PStoreConfigBuilder<V>(new JacksonSerializer<V>(mapper, clazz));
  }

  public static class PStoreConfigBuilder<V> {
    String name;
    PClassSerializer<V> serializer;

    PStoreConfigBuilder(PClassSerializer<V> serializer) {
      super();
      this.serializer = serializer;
    }

    public <X extends Builder> PStoreConfigBuilder<V> name(String name) {
      this.name = name;
      return this;
    }

    public PStoreConfig<V> build(){
      Preconditions.checkNotNull(name);
      return new PStoreConfig<V>(name, serializer);
    }

  }

}
