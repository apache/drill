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
package org.apache.drill.exec.store.paimon;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.drill.common.exceptions.UserException;
import org.apache.paimon.table.source.Split;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Objects;
import java.util.StringJoiner;

@JsonSerialize(using = PaimonWork.PaimonWorkSerializer.class)
@JsonDeserialize(using = PaimonWork.PaimonWorkDeserializer.class)
public class PaimonWork {
  private final Split split;

  public PaimonWork(Split split) {
    this.split = split;
  }

  public Split getSplit() {
    return split;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PaimonWork that = (PaimonWork) o;
    return Objects.equals(split, that.split);
  }

  @Override
  public int hashCode() {
    return Objects.hash(split);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", PaimonWork.class.getSimpleName() + "[", "]")
      .add("split=" + split)
      .toString();
  }

  public static class PaimonWorkDeserializer extends StdDeserializer<PaimonWork> {

    private static final Logger logger = LoggerFactory.getLogger(PaimonWorkDeserializer.class);

    public PaimonWorkDeserializer() {
      super(PaimonWork.class);
    }

    @Override
    public PaimonWork deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      JsonNode node = p.getCodec().readTree(p);
      String splitString = node.get(PaimonWorkSerializer.SPLIT_FIELD).asText();

      byte[] decoded = Base64.getDecoder().decode(splitString);
      try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(decoded))) {
        Object split = ois.readObject();
        if (!(split instanceof Split)) {
          throw UserException.dataReadError()
            .message("Deserialized object is not a Paimon Split: %s", split.getClass().getName())
            .build(logger);
        }
        return new PaimonWork((Split) split);
      } catch (ClassNotFoundException e) {
        logger.error("Failed to deserialize Paimon Split: {}", e.getMessage(), e);
        throw UserException.dataReadError(e)
          .message("Failed to deserialize Paimon Split: %s", e.getMessage())
          .build(logger);
      }
    }
  }

  public static class PaimonWorkSerializer extends StdSerializer<PaimonWork> {

    public static final String SPLIT_FIELD = "split";

    public PaimonWorkSerializer() {
      super(PaimonWork.class);
    }

    @Override
    public void serialize(PaimonWork value, JsonGenerator gen, SerializerProvider provider) throws IOException {
      gen.writeStartObject();
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
           ObjectOutputStream oos = new ObjectOutputStream(baos)) {
        oos.writeObject(value.split);
        oos.flush();
        gen.writeStringField(SPLIT_FIELD, Base64.getEncoder().encodeToString(baos.toByteArray()));
      }
      gen.writeEndObject();
    }
  }
}