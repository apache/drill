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


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = HttpXmlOptions.HttpXmlOptionsBuilder.class)
public class HttpXmlOptions {

  @JsonProperty
  private final int dataLevel;

  @JsonProperty
  private final TupleMetadata schema;

  @JsonCreator
  public HttpXmlOptions(@JsonProperty("dataLevel") Integer dataLevel,
                        @JsonProperty("schema") TupleMetadata schema) {
    this.schema = schema;
    if (dataLevel == null || dataLevel < 1) {
      this.dataLevel = 1;
    } else {
      this.dataLevel = dataLevel;
    }
  }

  public HttpXmlOptions(HttpXmlOptionsBuilder builder) {
    this.dataLevel = builder.dataLevel;
    this.schema = builder.schema;
  }


  public static HttpXmlOptionsBuilder builder() {
    return new HttpXmlOptionsBuilder();
  }

  @JsonProperty("dataLevel")
  public int getDataLevel() {
    return this.dataLevel;
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
    HttpXmlOptions that = (HttpXmlOptions) o;
    return Objects.equals(dataLevel, that.dataLevel)
      && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataLevel, schema);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("dataLevel", dataLevel)
      .field("schema", schema)
      .toString();
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class HttpXmlOptionsBuilder {

    private int dataLevel;
    private TupleMetadata schema;

    public HttpXmlOptions.HttpXmlOptionsBuilder dataLevel(int dataLevel) {
      this.dataLevel = dataLevel;
      return this;
    }

    public HttpXmlOptions.HttpXmlOptionsBuilder schema(TupleMetadata schema) {
      this.schema = schema;
      return this;
    }

    public HttpXmlOptions build() {
      return new HttpXmlOptions(this);
    }
  }
}
