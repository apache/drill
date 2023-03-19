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

package org.apache.drill.exec.store.http.paginator;

import okhttp3.HttpUrl.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.http.HttpPaginatorConfig.PaginatorMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class IndexPaginator extends Paginator {

  private static final Logger logger = LoggerFactory.getLogger(IndexPaginator.class);
  private final String hasMoreParam;
  private final String indexParam;
  private final String nextPageParam;

  private String indexValue;
  private String lastIndexValue;
  private String lastNextPageValue;
  private Boolean hasMoreValue;
  private String nextPageValue;
  private int pageCount;

  public IndexPaginator(Builder builder, int pageSize, int limit, String hasMoreParam, String indexParam, String nextPageParam) {
    super(builder, PaginatorMethod.INDEX, pageSize, limit);
    this.hasMoreParam = hasMoreParam;
    this.indexParam = indexParam;
    this.nextPageParam = nextPageParam;
    this.lastNextPageValue = "";
    this.lastIndexValue = "";
    pageCount = 0;
  }

  @Override
  public boolean hasNext() {
    return !partialPageReceived;
  }

  public String getIndexParam() {
    return this.indexParam;
  }

  public String getHasMoreParam() {
    return this.hasMoreParam;
  }

  public String getNextPageParam() {
    return this.nextPageParam;
  }

  public void setHasMoreValue(Boolean hasMoreValue) {
    this.hasMoreValue = hasMoreValue;
  }

  public void setIndexValue(String indexValue) {
    if (StringUtils.isNumeric(indexValue)) {

    }
    this.indexValue = indexValue;
  }

  public void setNextPageValue(String nextPageValue) {
    this.lastNextPageValue = this.nextPageValue;
    this.nextPageValue = nextPageValue;
  }

  public String getLastIndexValue() {
    return lastIndexValue;
  }

  public String getLastNextPageValue() {
    return lastNextPageValue;
  }

  public boolean isFirstPage() {
    return pageCount < 1;
  }

  @Override
  public String next() {
    // If the paginator has never been run before, just return the base URL.
    if (pageCount == 0) {
      pageCount++;
      return builder.build().url().toString();
    }

    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    if (StringUtils.isNotEmpty(indexValue)) {
      try {
        indexValue = URLEncoder.encode(indexValue, StandardCharsets.UTF_8.name());
      } catch (UnsupportedEncodingException e) {
        // Should never happen
        throw UserException.internalError()
          .message(e.getMessage())
          .build(logger);
      }
      builder.removeAllEncodedQueryParameters(indexParam);
      builder.addQueryParameter(indexParam, indexValue);
    } else if (StringUtils.isNotEmpty(nextPageValue)) {
      // Case when we're using the next page URL.  We have two cases here:
      // 1.  The nextPage contains the full URL of the next page.
      // 2.  The next page contains the path but lacks the base URL.
      if (StringUtils.startsWith(nextPageValue, "http://") ||
          StringUtils.startsWith(nextPageValue, "https://")) {
        pageCount++;
        return nextPageValue;
      } else {
        // If the next page just contains the path, we have to reconstruct a URL from the incoming path.
        int segmentIndex = 0;

        // Remove leading slash in path to avoid double slashes in URL
        if (nextPageValue.startsWith("/")) {
          nextPageValue = nextPageValue.substring(1);
        }

        // Now remove the path segments and replace with the updated ones from the URL.
        for (String segment : builder.build().pathSegments()) {
          if (nextPageValue.contains(segment)) {
            for (int i = builder.build().pathSegments().size() - 1; i >= segmentIndex; i--) {
              builder.removePathSegment(i);
            }
            break;
          }
          segmentIndex++;
        }
        builder.addPathSegments(nextPageValue);
      }

    }
    pageCount++;
    return builder.build().url().toString();
  }
}
