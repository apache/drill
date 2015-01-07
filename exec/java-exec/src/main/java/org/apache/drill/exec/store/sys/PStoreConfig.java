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
  private final Mode mode;
  private final int maxIteratorSize;

  public static enum Mode {PERSISTENT, EPHEMERAL, BLOB_PERSISTENT};

  private PStoreConfig(String name, PClassSerializer<V> valueSerializer, Mode mode, int maxIteratorSize) {
    super();
    this.name = name;
    this.valueSerializer = valueSerializer;
    this.mode = mode;
    this.maxIteratorSize = Math.abs(maxIteratorSize);
  }

  public Mode getMode() {
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

  public static <V extends Message, X extends Builder> PStoreConfigBuilder<V> newProtoBuilder(Schema<V> writeSchema, Schema<X> readSchema) {
    return new PStoreConfigBuilder<V>(new ProtoSerializer<V, X>(writeSchema, readSchema));
  }

  public static <V> PStoreConfigBuilder<V> newJacksonBuilder(ObjectMapper mapper, Class<V> clazz) {
    return new PStoreConfigBuilder<V>(new JacksonSerializer<V>(mapper, clazz));
  }

  public static class PStoreConfigBuilder<V> {
    String name;
    PClassSerializer<V> serializer;
    Mode mode = Mode.PERSISTENT;
    int maxIteratorSize = Integer.MAX_VALUE;

    PStoreConfigBuilder(PClassSerializer<V> serializer) {
      super();
      this.serializer = serializer;
    }

    public PStoreConfigBuilder<V> name(String name) {
      this.name = name;
      return this;
    }

    public PStoreConfigBuilder<V> persist(){
      this.mode = Mode.PERSISTENT;
      return this;
    }

    public PStoreConfigBuilder<V> ephemeral(){
      this.mode = Mode.EPHEMERAL;
      return this;
    }

    public PStoreConfigBuilder<V> blob(){
      this.mode = Mode.BLOB_PERSISTENT;
      return this;
    }

    /**
     * Set the maximum size of the iterator.  Positive numbers start from the start of the list.  Negative numbers start from the end of the list.
     * @param size
     * @return
     */
    public PStoreConfigBuilder<V> max(int size){
      this.maxIteratorSize = size;
      return this;
    }

    public PStoreConfig<V> build(){
      Preconditions.checkNotNull(name);
      return new PStoreConfig<V>(name, serializer, mode, maxIteratorSize);
    }

  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + maxIteratorSize;
    result = prime * result + ((mode == null) ? 0 : mode.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((valueSerializer == null) ? 0 : valueSerializer.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    PStoreConfig other = (PStoreConfig) obj;
    if (maxIteratorSize != other.maxIteratorSize) {
      return false;
    }
    if (mode != other.mode) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (valueSerializer == null) {
      if (other.valueSerializer != null) {
        return false;
      }
    } else if (!valueSerializer.equals(other.valueSerializer)) {
      return false;
    }
    return true;
  }


}
