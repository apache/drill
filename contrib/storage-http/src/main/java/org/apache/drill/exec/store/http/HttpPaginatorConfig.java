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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.UserException;


@Slf4j
@Builder
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@ToString
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = HttpPaginatorConfig.HttpPaginatorBuilder.class)
public class HttpPaginatorConfig {

  // For Offset Pagination
  @JsonProperty
  private final String limitParam;

  @JsonProperty
  private final String offsetParam;

  // For Page Pagination
  @JsonProperty
  private final String pageParam;

  @JsonProperty
  private final String pageSizeParam;

  @JsonProperty
  private final int pageSize;

  @JsonProperty
  private final int maxRecords;

  @JsonProperty
  private final String method;

  public enum PaginatorMethod {
    OFFSET,
    PAGE
  }

  private HttpPaginatorConfig(HttpPaginatorConfig.HttpPaginatorBuilder builder) {
    this.limitParam = builder.limitParam;
    this.offsetParam = builder.offsetParam;
    this.pageSize = builder.pageSize;
    this.pageParam = builder.pageParam;
    this.pageSizeParam = builder.pageSizeParam;
    this.maxRecords = builder.maxRecords;

    this.method = StringUtils.isEmpty(builder.method)
      ? PaginatorMethod.OFFSET.toString() : builder.method.trim().toUpperCase();

    PaginatorMethod paginatorMethod = PaginatorMethod.valueOf(this.method);

    /*
    * For pagination to function key fields must be defined.  This block validates the required fields for
    * each type of paginator.
     */
    switch (paginatorMethod) {
      case OFFSET:
        if (StringUtils.isEmpty(this.limitParam) || StringUtils.isEmpty(this.offsetParam)) {
          throw UserException
            .validationError()
            .message("Invalid paginator configuration.  For OFFSET pagination, limitField and offsetField must be defined.")
            .build(logger);
        } else if (this.pageSize <= 0) {
          throw UserException
            .validationError()
            .message("Invalid paginator configuration.  For OFFSET pagination, maxPageSize must be defined and greater than zero.")
            .build(logger);
        }
        break;
      case PAGE:
        if (StringUtils.isEmpty(this.pageParam) || StringUtils.isEmpty(this.pageSizeParam)) {
          throw UserException
            .validationError()
            .message("Invalid paginator configuration.  For PAGE pagination, pageField and pageSizeField must be defined.")
            .build(logger);
        } else if (this.pageSize <= 0) {
          throw UserException
            .validationError()
            .message("Invalid paginator configuration.  For PAGE pagination, maxPageSize must be defined and greater than zero.")
            .build(logger);
        }
        break;
      default:
        throw UserException
          .validationError()
          .message("Invalid paginator method: %s.  Drill supports 'OFFSET' and 'PAGE'", method)
          .build(logger);
    }
  }

  @JsonIgnore
  public PaginatorMethod getMethodType() {
    return PaginatorMethod.valueOf(this.method.toUpperCase());
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class HttpPaginatorBuilder {
    @Getter
    @Setter
    public String limitParam;

    @Getter
    @Setter
    public String offsetParam;

    @Getter
    @Setter
    public int maxRecords;

    @Getter
    @Setter
    public int pageSize;

    @Getter
    @Setter
    public String pageParam;

    @Getter
    @Setter
    public String pageSizeParam;

    @Getter
    @Setter
    public String method;

    public HttpPaginatorConfig build() {
      return new HttpPaginatorConfig(this);
    }
  }
}
