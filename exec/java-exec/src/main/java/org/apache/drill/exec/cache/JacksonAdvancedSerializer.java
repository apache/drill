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

import org.apache.drill.common.util.DataInputInputStream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

public class JacksonAdvancedSerializer<T> implements StreamSerializer<T>  {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JacksonAdvancedSerializer.class);

  private final Class<?> clazz;
  private final ObjectMapper mapper;
  private final int id;

  public JacksonAdvancedSerializer(SerializationDefinition def, ObjectMapper mapper){
    this.clazz =  def.clazz;
    this.mapper = mapper;
    this.id = def.id;
  }

  @Override
  public int getTypeId() {
    return id;
  }

  @Override
  public void destroy() {
  }

  @Override
  public void write(ObjectDataOutput out, T object) throws IOException {
    out.write(mapper.writeValueAsBytes(object));
  }

  @SuppressWarnings("unchecked")
  @Override
  public T read(ObjectDataInput in) throws IOException {
    return (T) mapper.readValue(DataInputInputStream.constructInputStream(in), clazz);
  }




}
