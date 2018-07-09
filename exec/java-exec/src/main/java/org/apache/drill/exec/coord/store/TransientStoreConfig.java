/*
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
package org.apache.drill.exec.coord.store;

import com.dyuproject.protostuff.Schema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.Message;
import org.apache.drill.exec.serialization.JacksonSerializer;
import org.apache.drill.exec.serialization.ProtoSerializer;
import org.apache.drill.exec.serialization.InstanceSerializer;

import java.util.Objects;

public class TransientStoreConfig<V> {
  private final String name;
  private final InstanceSerializer<V> serializer;

  protected TransientStoreConfig(String name, InstanceSerializer<V> serializer) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "name is required");
    this.name = name;
    this.serializer = Objects.requireNonNull(serializer, "serializer cannot be null");
  }

  public String getName() {
    return name;
  }

  public InstanceSerializer<V> getSerializer() {
    return serializer;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, serializer);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TransientStoreConfig && obj.getClass().equals(getClass())) {
      @SuppressWarnings("unchecked")
      TransientStoreConfig<V> other = (TransientStoreConfig<V>)obj;
      return Objects.equals(name, other.name) && Objects.equals(serializer, other.serializer);
    }
    return false;
  }

  public static <V> TransientStoreConfigBuilder<V> newBuilder() {
    return new TransientStoreConfigBuilder<>();
  }

  public static <V extends Message, B extends Message.Builder> TransientStoreConfigBuilder<V> newProtoBuilder(Schema<V> writeSchema, Schema<B> readSchema) {
    return TransientStoreConfig.<V>newBuilder().serializer(new ProtoSerializer<>(readSchema, writeSchema));
  }

  public static <V> TransientStoreConfigBuilder<V> newJacksonBuilder(ObjectMapper mapper, Class<V> klazz) {
    return TransientStoreConfig.<V>newBuilder().serializer(new JacksonSerializer<>(mapper, klazz));
  }
}
