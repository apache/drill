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
package org.apache.drill.exec.store.sys.serialize;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

public class JacksonSerializer<X> implements PClassSerializer<X> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JacksonSerializer.class);

  private ObjectWriter writer;
  private ObjectReader reader;

  public JacksonSerializer(ObjectMapper mapper, Class<X> clazz){
    this.reader = mapper.reader(clazz);
    this.writer = mapper.writer();
  }

  @Override
  public byte[] serialize(X val) throws IOException {
    return writer.writeValueAsBytes(val);
  }

  @Override
  public X deserialize(byte[] bytes) throws IOException {
    return reader.readValue(bytes);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((reader == null) ? 0 : reader.hashCode());
    result = prime * result + ((writer == null) ? 0 : writer.hashCode());
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
    JacksonSerializer other = (JacksonSerializer) obj;
    if (reader == null) {
      if (other.reader != null) {
        return false;
      }
    } else if (!reader.equals(other.reader)) {
      return false;
    }
    if (writer == null) {
      if (other.writer != null) {
        return false;
      }
    } else if (!writer.equals(other.writer)) {
      return false;
    }
    return true;
  }


}
