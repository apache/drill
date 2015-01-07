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

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.dyuproject.protostuff.JsonIOUtil;
import com.dyuproject.protostuff.Schema;
import com.google.protobuf.Message;

public class ProtoSerializer<X, B extends Message.Builder> implements PClassSerializer<X> {

  private final Schema<X> writeSchema;
  private final Schema<B> readSchema;

  public ProtoSerializer(Schema<X> writeSchema, Schema<B> readSchema) {
    super();
    this.writeSchema = writeSchema;
    this.readSchema = readSchema;
  }

  @Override
  public byte[] serialize(X val) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JsonIOUtil.writeTo(baos, val, writeSchema, false);
    return baos.toByteArray();
  }

  @SuppressWarnings("unchecked")
  @Override
  public X deserialize(byte[] bytes) throws IOException {
    B b = readSchema.newMessage();
    JsonIOUtil.mergeFrom(bytes, b, readSchema, false);
    return (X) b.build();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((readSchema == null) ? 0 : readSchema.hashCode());
    result = prime * result + ((writeSchema == null) ? 0 : writeSchema.hashCode());
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
    ProtoSerializer other = (ProtoSerializer) obj;
    if (readSchema == null) {
      if (other.readSchema != null) {
        return false;
      }
    } else if (!readSchema.equals(other.readSchema)) {
      return false;
    }
    if (writeSchema == null) {
      if (other.writeSchema != null) {
        return false;
      }
    } else if (!writeSchema.equals(other.writeSchema)) {
      return false;
    }
    return true;
  }


}
