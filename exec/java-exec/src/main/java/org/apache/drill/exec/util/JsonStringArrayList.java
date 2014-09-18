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
package org.apache.drill.exec.util;

import java.util.ArrayList;

import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class JsonStringArrayList<E> extends ArrayList<E> {

  private static ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
    SimpleModule serializer = new SimpleModule("TextSerializer")
        .addSerializer(Text.class, new TextSerializer());
    mapper.registerModule(serializer);
  }

  @Override
  public final String toString() {
    try {
      return mapper.writeValueAsString(this);
    } catch(JsonProcessingException e) {
      throw new IllegalStateException("Cannot serialize array list to JSON string", e);
    }
  }
}
