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
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.infinispan.commons.marshall.AdvancedExternalizer;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;

public class ProtobufAdvancedExternalizer<T extends Message> implements AdvancedExternalizer<T>  {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProtobufAdvancedExternalizer.class);

  private final Class<?> clazz;
  private final int id;
  private final Parser<T> parser;

  public ProtobufAdvancedExternalizer(SerializationDefinition def, Parser<T> parser){
    this.clazz =  def.clazz;
    this.parser = parser;
    this.id = def.id;
  }

  @Override
  public T readObject(ObjectInput in) throws IOException, ClassNotFoundException {
    return parser.parseFrom(DataInputInputStream.constructInputStream(in));
  }

  @Override
  public void writeObject(ObjectOutput out, T object) throws IOException {
    out.write(object.toByteArray());
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
