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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.MapBuilder;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.UnionBuilder;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderOptions;
import org.apache.drill.exec.store.http.providedSchema.HttpField;

import java.util.List;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = HttpJsonOptions.HttpJsonOptionsBuilder.class)
public class HttpJsonOptions {

  @JsonProperty
  private final Boolean allowNanInf;

  @JsonProperty
  private final Boolean allTextMode;

  @JsonProperty
  private final Boolean readNumbersAsDouble;

  @JsonProperty
  private final Boolean enableEscapeAnyChar;

  @JsonProperty
  private final Boolean skipMalformedRecords;

  @JsonProperty
  private final Boolean skipMalformedDocument;


  @JsonProperty
  private final List<HttpField> providedSchema;

  HttpJsonOptions(HttpJsonOptionsBuilder builder) {
    this.allowNanInf = builder.allowNanInf;
    this.allTextMode = builder.allTextMode;
    this.readNumbersAsDouble = builder.readNumbersAsDouble;
    this.enableEscapeAnyChar = builder.enableEscapeAnyChar;
    this.skipMalformedRecords = builder.skipMalformedRecords;
    this.skipMalformedDocument = builder.skipMalformedDocument;
    this.providedSchema = builder.providedSchema;
  }

  public static HttpJsonOptionsBuilder builder() {
    return new HttpJsonOptionsBuilder();
  }

  @JsonIgnore
  public JsonLoaderOptions getJsonOptions(OptionSet optionSet) {
    JsonLoaderOptions options = new JsonLoaderOptions(optionSet);
    if (allowNanInf != null) {
      options.allowNanInf = allowNanInf;
    }
    if (allTextMode != null) {
      options.allTextMode = allTextMode;
    }
    if (readNumbersAsDouble != null) {
      options.readNumbersAsDouble = readNumbersAsDouble;
    }
    if (enableEscapeAnyChar != null) {
      options.enableEscapeAnyChar = enableEscapeAnyChar;
    }
    if (skipMalformedDocument != null) {
      options.skipMalformedDocument = skipMalformedDocument;
    }
    if (skipMalformedRecords != null) {
      options.skipMalformedRecords = skipMalformedRecords;
    }
    return options;
  }

  @JsonIgnore
  public TupleMetadata buildSchema() {
    SchemaBuilder schemaBuilder = new SchemaBuilder();
    for (HttpField field : providedSchema) {
      addField(schemaBuilder, field);
    }
    return schemaBuilder.build();
  }

  private void addField(SchemaBuilder schemaBuilder, HttpField field) {
    if (isCompoundField(field.getDrillType())) {
      if (field.getDrillType() == MinorType.MAP) {
        MapBuilder innerMapBuilder = schemaBuilder.addMap(field.getFieldName());
        for (HttpField innerField : field.getFields()) {
          addField(innerMapBuilder, innerField);
        }
        innerMapBuilder.resumeSchema();
      } /*else if (field.getDrillType() == MinorType.UNION) {
        UnionBuilder innerUnionBuilder = schemaBuilder.addUnion(field.getFieldName());
        for (HttpField innerField : field.getFields()) {
          addField(innerUnionBuilder, innerField);
        }
        innerUnionBuilder.resumeSchema();
      }*/
    } else {
      schemaBuilder.addNullable(field.getFieldName(), field.getDrillType());
    }
  }

  private void addField(MapBuilder builder, HttpField field) {
    if (isCompoundField(field.getDrillType())) {
      if (field.getDrillType() == MinorType.MAP) {
        MapBuilder innerMapBuilder = builder.addMap(field.getFieldName());
        for (HttpField innerField : field.getFields()) {
          addField(innerMapBuilder, innerField);
        }
        innerMapBuilder.resumeMap();
      }
    } else {
      builder.addNullable(field.getFieldName(), field.getDrillType());
    }
  }

  /*
  private void addField(UnionBuilder builder, HttpField field) {
    if (isCompoundField(field.getDrillType())) {
      if (field.getDrillType() == MinorType.MAP) {
        MapBuilder innerMapBuilder = builder.addMap();
        for (HttpField innerField : field.getFields()) {
          addField(innerMapBuilder, innerField);
        } else if (field.getDrillType() == MinorType.UNION) {
          UnionBuilder innerUnionBuilder = builder.addUnion();
          for (HttpField innerField : field.getFields()) {
            addField(innerUnionBuilder, innerField);
          }
          innerUnionBuilder.resumeSchema();
        }
        innerUnionBuilder.resumeUnion();
      }
    } else {
      TupleMetadata columnMetadata = new SchemaBuilder()
        .addNullable(field.getFieldName(), field.getDrillType())
        .build();
      builder.addColumn(columnMetadata.metadata(0));
    }
  }*/

  public static boolean isCompoundField(MinorType type) {
    return type == MinorType.MAP || type == MinorType.UNION || type == MinorType.DICT || type == MinorType.LIST;
  }

  @JsonProperty("allowNanInf")
  public Boolean allowNanInf() {
    return this.allowNanInf;
  }

  @JsonProperty("allTextMode")
  public Boolean allTextMode() {
    return this.allTextMode;
  }

  @JsonProperty("readNumbersAsDouble")
  public Boolean readNumbersAsDouble() {
    return this.readNumbersAsDouble;
  }

  @JsonProperty("enableEscapeAnyChar")
  public Boolean enableEscapeAnyChar() {
    return this.enableEscapeAnyChar;
  }

  @JsonProperty("skipMalformedRecords")
  public Boolean skipMalformedRecords() {
    return this.skipMalformedRecords;
  }

  @JsonProperty("skipMalformedDocument")
  public Boolean skipMalformedDocument() {
    return this.skipMalformedDocument;
  }

  @JsonProperty("providedSchema")
  public List<HttpField> providedSchema() {
    return this.providedSchema;
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
      && Objects.equals(skipMalformedRecords, that.skipMalformedRecords);
  }

  @Override
  public int hashCode() {
    return Objects.hash(allowNanInf, allTextMode, readNumbersAsDouble, enableEscapeAnyChar, skipMalformedRecords, skipMalformedDocument);
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
      .toString();
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class HttpJsonOptionsBuilder {
    private Boolean allowNanInf;

    private Boolean allTextMode;

    private Boolean readNumbersAsDouble;

    private Boolean enableEscapeAnyChar;

    private Boolean skipMalformedRecords;

    private Boolean skipMalformedDocument;

    private List<HttpField> providedSchema;

    public HttpJsonOptionsBuilder allowNanInf(Boolean allowNanInf) {
      this.allowNanInf = allowNanInf;
      return this;
    }

    public HttpJsonOptionsBuilder allTextMode(Boolean allTextMode) {
      this.allTextMode = allTextMode;
      return this;
    }

    public HttpJsonOptionsBuilder readNumbersAsDouble(Boolean readNumbersAsDouble) {
      this.readNumbersAsDouble = readNumbersAsDouble;
      return this;
    }

    public HttpJsonOptionsBuilder enableEscapeAnyChar(Boolean enableEscapeAnyChar) {
      this.enableEscapeAnyChar = enableEscapeAnyChar;
      return this;
    }

    public HttpJsonOptionsBuilder skipMalformedRecords(Boolean skipMalformedRecords) {
      this.skipMalformedRecords = skipMalformedRecords;
      return this;
    }

    public HttpJsonOptionsBuilder skipMalformedDocument(Boolean skipMalformedDocument) {
      this.skipMalformedDocument = skipMalformedDocument;
      return this;
    }

    public HttpJsonOptionsBuilder providedSchema(List<HttpField> schema) {
      this.providedSchema = schema;
      return this;
    }

    public HttpJsonOptions build() {
      return new HttpJsonOptions(this);
    }
  }
}
