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

package org.apache.drill.exec.server.options;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.base.Preconditions;

import java.io.IOException;

// Custom Deserializer required for backward compatibility DRILL-5809
@JsonDeserialize(using = PersistedOptionValue.Deserializer.class)
public class PersistedOptionValue {
  public static final String JSON_VALUE = "value";

  private String value;

  public PersistedOptionValue(@JsonProperty(JSON_VALUE) String value) {
    this.value = Preconditions.checkNotNull(value);
  }

  public String getValue() {
    return value;
  }

  public OptionValue toOptionValue(final OptionDefinition optionDefinition, final OptionValue.OptionScope optionScope) {
    final OptionValidator validator = optionDefinition.getValidator();
    final OptionValue.Kind kind = validator.getKind();
    final String name = validator.getOptionName();
    final OptionValue.AccessibleScopes accessibleScopes = optionDefinition.getMetaData().getAccessibleScopes();

    return OptionValue.create(kind, accessibleScopes, name, value, optionScope);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PersistedOptionValue that = (PersistedOptionValue) o;

    return value.equals(that.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public String toString() {
    return "PersistedOptionValue{" + "value='" + value + '\'' + '}';
  }

  public static class Deserializer extends StdDeserializer<PersistedOptionValue> {
    private Deserializer() {
      super(PersistedOptionValue.class);
    }

    protected Deserializer(JavaType valueType) {
      super(valueType);
    }

    protected Deserializer(StdDeserializer<?> src) {
      super(src);
    }

    @Override
    public PersistedOptionValue deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      ObjectCodec oc = p.getCodec();
      JsonNode node = oc.readTree(p);
      String value = null;

      if (node.has(OptionValue.JSON_NUM_VAL)) {
        value = node.get(OptionValue.JSON_NUM_VAL).asText();
      }

      if (node.has(OptionValue.JSON_STRING_VAL)) {
        value = node.get(OptionValue.JSON_STRING_VAL).asText();
      }

      if (node.has(OptionValue.JSON_BOOL_VAL)) {
        value = node.get(OptionValue.JSON_BOOL_VAL).asText();
      }

      if (node.has(OptionValue.JSON_FLOAT_VAL)) {
        value = node.get(OptionValue.JSON_FLOAT_VAL).asText();
      }

      if (node.has(JSON_VALUE)) {
        value = node.get(JSON_VALUE).asText();
      }

      return new PersistedOptionValue(value);
    }
  }
}
