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

import com.dyuproject.protostuff.Schema;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import org.apache.drill.exec.store.sys.serialize.JacksonSerializer;
import org.apache.drill.exec.store.sys.serialize.PClassSerializer;
import org.apache.drill.exec.store.sys.serialize.ProtoSerializer;


/**
 * An abstraction for configurations that are used to create a {@link Store store}.
 *
 * @param <V>  value type of which {@link Store} uses to store & retrieve instances
 */
public class StoreConfig<V> {

  private final String name;
  private final PClassSerializer<V> valueSerializer;
  private final StoreMode mode;
  private final int maxIteratorSize;

  protected StoreConfig(String name, PClassSerializer<V> valueSerializer, StoreMode mode, int maxIteratorSize) {
    this.name = name;
    this.valueSerializer = valueSerializer;
    this.mode = mode;
    this.maxIteratorSize = Math.abs(maxIteratorSize);
  }

  public StoreMode getMode() {
    return mode;
  }

  public int getMaxIteratorSize() {
    return maxIteratorSize;
  }

  public String getName() {
    return name;
  }

  public PClassSerializer<V> getSerializer() {
    return valueSerializer;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, valueSerializer, mode, maxIteratorSize);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof StoreConfig) {
      final StoreConfig other = StoreConfig.class.cast(obj);
      return Objects.equal(name, other.name)
          && Objects.equal(valueSerializer, other.valueSerializer)
          && Objects.equal(mode, other.mode)
          && Objects.equal(maxIteratorSize, other.maxIteratorSize);
    }
    return false;
  }

  public static <V extends Message, X extends Builder> StoreConfigBuilder<V> newProtoBuilder(Schema<V> writeSchema, Schema<X> readSchema) {
    return new StoreConfigBuilder<>(new ProtoSerializer<>(writeSchema, readSchema));
  }

  public static <V> StoreConfigBuilder<V> newJacksonBuilder(ObjectMapper mapper, Class<V> clazz) {
    return new StoreConfigBuilder<>(new JacksonSerializer<>(mapper, clazz));
  }

  public static class StoreConfigBuilder<V> {
    private String name;
    private PClassSerializer<V> serializer;
    private StoreMode mode = StoreMode.PERSISTENT;
    private int maxSize = Integer.MAX_VALUE;

    protected StoreConfigBuilder(PClassSerializer<V> serializer) {
      super();
      this.serializer = serializer;
    }

    public StoreConfigBuilder<V> name(String name) {
      this.name = name;
      return this;
    }

    public StoreConfigBuilder<V> persist(){
      this.mode = StoreMode.PERSISTENT;
      return this;
    }

    public StoreConfigBuilder<V> ephemeral(){
      this.mode = StoreMode.EPHEMERAL;
      return this;
    }

    public StoreConfigBuilder<V> blob(){
      this.mode = StoreMode.BLOB_PERSISTENT;
      return this;
    }

    /**
     * Set the maximum size of the iterator.  Positive numbers start from the start of the list.  Negative numbers start from the end of the list.
     * @param size
     * @return
     */
    public StoreConfigBuilder<V> maxSize(int size){
      this.maxSize = size;
      return this;
    }

    public StoreConfig<V> build(){
      Preconditions.checkNotNull(name);
      return new StoreConfig<>(name, serializer, mode, maxSize);
    }

  }


}
