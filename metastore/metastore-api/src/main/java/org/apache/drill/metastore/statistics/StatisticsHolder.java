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
package org.apache.drill.metastore.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.IOException;

/**
 * Class-holder for statistics kind and its value.
 *
 * @param <T> Type of statistics value
 */
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class StatisticsHolder<T> {

  static final ObjectWriter OBJECT_WRITER = new ObjectMapper().setDefaultPrettyPrinter(new DefaultPrettyPrinter()).writer();
  private static final ObjectReader OBJECT_READER = new ObjectMapper().readerFor(StatisticsHolder.class);

  private final T statisticsValue;
  private final BaseStatisticsKind statisticsKind;

  @JsonCreator
  public StatisticsHolder(@JsonProperty("statisticsValue") T statisticsValue,
                          @JsonProperty("statisticsKind") BaseStatisticsKind statisticsKind) {
    this.statisticsValue = statisticsValue;
    this.statisticsKind = statisticsKind;
  }

  public StatisticsHolder(T statisticsValue,
                          StatisticsKind statisticsKind) {
    this.statisticsValue = statisticsValue;
    this.statisticsKind = (BaseStatisticsKind) statisticsKind;
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS,
                include = JsonTypeInfo.As.WRAPPER_OBJECT)
  public T getStatisticsValue() {
    return statisticsValue;
  }

  public StatisticsKind getStatisticsKind() {
    return statisticsKind;
  }

  public static StatisticsHolder of(String serialized) throws IOException {
    return OBJECT_READER.readValue(serialized);
  }

  public String jsonString() throws JsonProcessingException {
    return OBJECT_WRITER.writeValueAsString(this);
  }
}
