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
package org.apache.drill.exec.cache.infinispan;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.apache.drill.common.util.DataInputInputStream;
import org.apache.drill.exec.cache.SerializationDefinition;
import org.infinispan.commons.marshall.AdvancedExternalizer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

public class JacksonAdvancedExternalizer<T> implements AdvancedExternalizer<T>  {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JacksonAdvancedExternalizer.class);

  private final Class<?> clazz;
  private final ObjectMapper mapper;
  private final int id;

  public JacksonAdvancedExternalizer(SerializationDefinition def, ObjectMapper mapper){
    this.clazz =  def.clazz;
    this.mapper = mapper;
    this.id = def.id;
  }

  @Override
  public T readObject(ObjectInput in) throws IOException, ClassNotFoundException {
    byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    return (T) mapper.readValue(bytes, clazz);
  }

  @Override
  public void writeObject(ObjectOutput out, T object) throws IOException {
    byte[] bytes = mapper.writeValueAsBytes(object);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  @Override
  public Integer getId() {
    return id;
  }

  @Override
  public Set<Class<? extends T>> getTypeClasses() {
    return (Set<Class<? extends T>>) (Object) Collections.singleton(clazz);
  }




}
