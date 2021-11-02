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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.options.OptionSet;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderOptions;

@Slf4j
@Builder
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = HttpJsonOptions.HttpJsonOptionsBuilder.class)
public class HttpJsonOptions {

  @JsonInclude
  private final boolean allowNanInf;

  @JsonInclude
  private final boolean allTextMode;

  @JsonInclude
  private final boolean readNumbersAsDouble;

  @JsonInclude
  private final boolean enableEscapeAnyChar;

  @JsonIgnore
  public JsonLoaderOptions getJsonOptions(OptionSet optionSet) {

    JsonLoaderOptions options = new JsonLoaderOptions(optionSet);

    if (optionSet.getBoolean(ExecConstants.JSON_READER_NAN_INF_NUMBERS) != allowNanInf) {
      options.allowNanInf = allowNanInf;
    }

    if (optionSet.getBoolean(ExecConstants.JSON_ALL_TEXT_MODE) != allTextMode) {
      options.allTextMode = allTextMode;
    }

    if (optionSet.getBoolean(ExecConstants.JSON_READ_NUMBERS_AS_DOUBLE) != readNumbersAsDouble) {
      options.readNumbersAsDouble = readNumbersAsDouble;
    }

    if (optionSet.getBoolean(ExecConstants.JSON_READER_ESCAPE_ANY_CHAR) != enableEscapeAnyChar) {
      options.enableEscapeAnyChar = enableEscapeAnyChar;
    }

    return options;
  }

}
