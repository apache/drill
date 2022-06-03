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

package org.apache.drill.exec.store.http;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.easy.json.config.JsonConfigOptions;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = HttpJsonOptions.HttpJsonOptionsBuilder.class)
public class HttpJsonOptions extends JsonConfigOptions {
  @JsonProperty
  private final TupleMetadata schema;

  HttpJsonOptions(HttpJsonOptionsBuilder builder) {
    super(builder.allowNanInf, builder.allTextMode, builder.readNumbersAsDouble, builder.enableEscapeAnyChar, builder.skipMalformedDocument, builder.skipMalformedRecords);
    this.schema = builder.schema;
  }

  public static HttpJsonOptionsBuilder builder() {
    return new HttpJsonOptionsBuilder();
  }


  @JsonProperty("schema")
  public TupleMetadata schema() {
    return this.schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HttpJsonOptions that = (HttpJsonOptions) o;
    return Objects.equals(allowNanInf, that.allowNanInf)
      && Objects.equals(allTextMode, that.allTextMode)
      && Objects.equals(readNumbersAsDouble, that.readNumbersAsDouble)
      && Objects.equals(enableEscapeAnyChar, that.enableEscapeAnyChar)
      && Objects.equals(skipMalformedDocument, that.skipMalformedDocument)
      && Objects.equals(skipMalformedRecords, that.skipMalformedRecords)
      && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(allowNanInf, allTextMode, readNumbersAsDouble, enableEscapeAnyChar, skipMalformedDocument, skipMalformedRecords, schema);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("allowNanInf", allowNanInf)
      .field("allTextMode", allTextMode)
      .field("readNumbersAsDouble", readNumbersAsDouble)
      .field("enableEscapeAnyChar", enableEscapeAnyChar)
      .field("skipMalformedRecords", skipMalformedRecords)
      .field("skipMalformedDocument", skipMalformedDocument)
      .field("schema", schema)
      .toString();
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class HttpJsonOptionsBuilder extends JsonConfigOptionsBuilder {
    private TupleMetadata schema;

    public HttpJsonOptionsBuilder schema(TupleMetadata schema) {
      this.schema = schema;
      return this;
    }

    public HttpJsonOptionsBuilder allTextMode(Boolean allTextMode) {
      super.allTextMode(allTextMode);
      return this;
    }

    public HttpJsonOptionsBuilder allowNanInf(Boolean allowNanInf) {
      super.allowNanInf(allowNanInf);
      return this;
    }

    public HttpJsonOptionsBuilder enableEscapeAnyChar(Boolean enableEscapeAnyChar) {
      super.enableEscapeAnyChar(enableEscapeAnyChar);
      return this;
    }

    public HttpJsonOptionsBuilder readNumbersAsDouble(Boolean readNumbersAsDouble) {
      super.readNumbersAsDouble(readNumbersAsDouble);
      return this;
    }

    public HttpJsonOptionsBuilder skipMalformedRecords(Boolean skipMalformedRecords) {
      super.skipMalformedRecords(skipMalformedRecords);
      return this;
    }

    public HttpJsonOptionsBuilder skipMalformedDocument(Boolean skipMalformedDocument) {
      super.skipMalformedDocument(skipMalformedDocument);
      return this;
    }

    public HttpJsonOptions build() {
      return new HttpJsonOptions(this);
    }
  }
}
