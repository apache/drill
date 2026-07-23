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
package org.apache.drill.common.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;

/**
 * Utility class which contain methods for interacting with Jackson.
 */
public final class JacksonUtils {

  private JacksonUtils() {}

  /**
   * Creates a new instance of the Jackson {@link ObjectMapper}.
   * @return an {@link ObjectMapper} instance
   */
  public static ObjectMapper createObjectMapper() {
    return createJsonMapperBuilder().build();
  }

  /**
   * Creates a new instance of the Jackson {@link ObjectMapper}.
   * @param factory a {@link JsonFactory} instance
   * @return an {@link ObjectMapper} instance
   */
  public static ObjectMapper createObjectMapper(final JsonFactory factory) {
    return createJsonMapperBuilder(factory).build();
  }

  /**
   * Creates a new instance of the Jackson {@link JsonMapper.Builder}.
   * @return an {@link JsonMapper.Builder} instance
   */
  public static JsonMapper.Builder createJsonMapperBuilder() {
    // it is deliberate to have polymorphicTypeValidator that allows nothing
    // for security reasons
    // org.apache.drill.metastore.statistics.StatisticsHolder replaces this with
    // a polymorphicTypeValidator that allows only the types it needs
    return JsonMapper.builder()
        .polymorphicTypeValidator(createPolymorphicTypeValidator());
  }

  /**
   * Creates a new instance of the Jackson {@link JsonMapper.Builder}.
   * @param factory a {@link JsonFactory} instance
   * @return an {@link JsonMapper.Builder} instance
   */
  public static JsonMapper.Builder createJsonMapperBuilder(final JsonFactory factory) {
    return JsonMapper.builder(factory)
        .polymorphicTypeValidator(createPolymorphicTypeValidator());
  }

  // The more restrictive this validator is, the better for security.
  private static PolymorphicTypeValidator createPolymorphicTypeValidator() {
    return BasicPolymorphicTypeValidator.builder()
        .allowIfSubType(Number.class)
        .allowIfSubType(Boolean.class)
        .allowIfSubType(String.class)
        .allowIfSubType(byte[].class)
        .allowIfSubType("java.time.")
        .allowIfSubType("org.joda.time.") // Joda used by ColumnStatistics
        .allowIfSubType("org.apache.drill.exec.")
        .allowIfSubType("org.apache.drill.metastore.")
        .build();
  }

}
