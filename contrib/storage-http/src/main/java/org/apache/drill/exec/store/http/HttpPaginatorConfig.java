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
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.UserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;


@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonDeserialize(builder = HttpPaginatorConfig.HttpPaginatorConfigBuilder.class)
public class HttpPaginatorConfig {

  private static final Logger logger = LoggerFactory.getLogger(HttpPaginatorConfig.class);

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

  // For index/keyset pagination
  @JsonProperty
  private final String hasMoreParam;

  @JsonProperty
  private final String indexParam;

  @JsonProperty
  private final String nextPageParam;

  public HttpPaginatorConfig(HttpPaginatorConfigBuilder builder) {
    this.limitParam = builder.limitParam;
    this.offsetParam = builder.offsetParam;
    this.pageParam = builder.pageParam;
    this.pageSizeParam = builder.pageSizeParam;
    this.pageSize = builder.pageSize;
    this.maxRecords = builder.maxRecords;
    this.hasMoreParam = builder.hasMoreParam;
    this.indexParam = builder.indexParam;
    this.nextPageParam = builder.nextPageParam;

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
      case INDEX:
        // Either the nextPageParam OR the indexParam must be populated
        if ((StringUtils.isEmpty(this.nextPageParam) && StringUtils.isNotEmpty(this.indexParam)) &&
          (StringUtils.isNotEmpty(this.nextPageParam) && StringUtils.isEmpty(this.indexParam))) {
          throw UserException
            .validationError()
            .message("Invalid paginator configuration.  For INDEX pagination, the nextPageParam or indexParam must be defined.")
            .build(logger);
        } else if (StringUtils.isEmpty(this.nextPageParam) && StringUtils.isEmpty(this.indexParam)) {
          throw UserException
            .validationError()
            .message("Invalid paginator configuration.  For INDEX pagination, the nextPageParam or indexParam must be defined.")
            .build(logger);
        }
        break;
      default:
        throw UserException
          .validationError()
          .message("Invalid paginator method: %s.  Drill supports 'OFFSET', 'INDEX' and 'PAGE'", method)
          .build(logger);
    }
  }

  public static HttpPaginatorConfigBuilder builder() {
    return new HttpPaginatorConfigBuilder();
  }

  public String limitParam() {
    return this.limitParam;
  }

  public String offsetParam() {
    return this.offsetParam;
  }

  public String pageParam() {
    return this.pageParam;
  }

  public String pageSizeParam() {
    return this.pageSizeParam;
  }

  public int pageSize() {
    return this.pageSize;
  }

  public int maxRecords() {
    return this.maxRecords;
  }

  public String method() {
    return this.method;
  }

  public String hasMoreParam() {
    return hasMoreParam;
  }

  public String nextPageParam() {
    return nextPageParam;
  }
  public String indexParam() {
    return indexParam;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HttpPaginatorConfig that = (HttpPaginatorConfig) o;
    return pageSize == that.pageSize
      && maxRecords == that.maxRecords
      && Objects.equals(limitParam, that.limitParam)
      && Objects.equals(offsetParam, that.offsetParam)
      && Objects.equals(pageParam, that.pageParam)
      && Objects.equals(pageSizeParam, that.pageSizeParam)
      && Objects.equals(method, that.method)
      && Objects.equals(hasMoreParam, that.hasMoreParam)
      && Objects.equals(indexParam, that.indexParam)
      && Objects.equals(nextPageParam, that.nextPageParam);
  }

  @Override
  public int hashCode() {
    return Objects.hash(limitParam, offsetParam, pageParam, pageSizeParam,
      pageSize, maxRecords, method, nextPageParam, indexParam, hasMoreParam);
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("limitParam", limitParam)
      .field("offsetParam", offsetParam)
      .field("pageParam", pageParam)
      .field("pageSizeParam", pageSizeParam)
      .field("pageSize", pageSize)
      .field("maxRecords", maxRecords)
      .field("method", method)
      .field("indexParam", indexParam)
      .field("hasMoreParam", hasMoreParam)
      .field("nextPageParam", nextPageParam)
      .toString();
  }

  public enum PaginatorMethod {
    OFFSET,
    PAGE,
    INDEX
  }

  @JsonIgnore
  public PaginatorMethod getMethodType() {
    return PaginatorMethod.valueOf(this.method.toUpperCase());
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class HttpPaginatorConfigBuilder {
    private String limitParam;
    private String offsetParam;
    private String pageParam;
    private String pageSizeParam;
    private int pageSize;
    private int maxRecords;
    private String method;
    private String hasMoreParam;
    private String indexParam;
    private String nextPageParam;

    public HttpPaginatorConfigBuilder hasMoreParam(String hasMoreParam) {
      this.hasMoreParam = hasMoreParam;
      return this;
    }

    public HttpPaginatorConfigBuilder indexParam(String indexParam) {
      this.indexParam = indexParam;
      return this;
    }

    public HttpPaginatorConfigBuilder nextPageParam(String nextPageParam) {
      this.nextPageParam = nextPageParam;
      return this;
    }

    public HttpPaginatorConfigBuilder limitParam(String limitParam) {
      this.limitParam = limitParam;
      return this;
    }

    public HttpPaginatorConfigBuilder offsetParam(String offsetParam) {
      this.offsetParam = offsetParam;
      return this;
    }

    public HttpPaginatorConfigBuilder pageParam(String pageParam) {
      this.pageParam = pageParam;
      return this;
    }

    public HttpPaginatorConfigBuilder pageSizeParam(String pageSizeParam) {
      this.pageSizeParam = pageSizeParam;
      return this;
    }

    public HttpPaginatorConfigBuilder pageSize(int pageSize) {
      this.pageSize = pageSize;
      return this;
    }

    public HttpPaginatorConfigBuilder maxRecords(int maxRecords) {
      this.maxRecords = maxRecords;
      return this;
    }

    public HttpPaginatorConfigBuilder method(String method) {
      this.method = method;
      return this;
    }

    public HttpPaginatorConfig build() {
      return new HttpPaginatorConfig(this);
    }
  }
}
