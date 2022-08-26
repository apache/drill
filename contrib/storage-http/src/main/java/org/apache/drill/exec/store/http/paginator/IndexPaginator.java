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
import org.apache.drill.exec.store.http.HttpPaginatorConfig.PaginatorMethod;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.NoSuchElementException;

public class IndexPaginator extends Paginator {

  private final String hasMoreParam;
  private final String indexParam;
  private final String nextPageParam;

  private String indexValue;
  private Boolean hasMoreValue;
  private String nextPageValue;
  private int pageCount;

  public IndexPaginator(Builder builder, int pageSize, int limit, String hasMoreParam, String indexParam, String nextPageParam) {
    super(builder, PaginatorMethod.INDEX, pageSize, limit);
    this.hasMoreParam = hasMoreParam;
    this.indexParam = indexParam;
    this.nextPageParam = nextPageParam;
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
    this.nextPageValue = nextPageValue;
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

    if (StringUtils.isNotEmpty(nextPageValue)) {
      // TODO figure this out...
    } else if (StringUtils.isNotEmpty(indexValue)) {
      try {
        indexValue = URLEncoder.encode(indexValue, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        // Do nothing.
      }
      builder.removeAllEncodedQueryParameters(indexParam);
      builder.addQueryParameter(indexParam, indexValue);
    }
    pageCount++;
    return builder.build().url().toString();
  }
}
