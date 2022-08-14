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
import org.apache.drill.exec.store.http.HttpPaginatorConfig.PaginatorMethod;

import java.util.NoSuchElementException;

public class IndexPaginator extends Paginator {

  private final String hasMoreParam;
  private final String indexParam;
  private final String nextPageParam;

  private String indexValue;
  private String hasMoreValue;
  private String nextPageValue;

  public IndexPaginator(Builder builder, int pageSize, int limit, String hasMoreParam, String indexParam, String nextPageParam) {
    super(builder, PaginatorMethod.INDEX, pageSize, limit);
    this.hasMoreParam = hasMoreParam;
    this.indexParam = indexParam;
    this.nextPageParam = nextPageParam;
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

  public void setIndexParam(String indexValue) {
    this.indexValue = indexValue;
  }

  public void setNextPageValue(String nextPageValue) {
    this.nextPageValue = nextPageValue;
  }

  public void setHasMoreValue(String hasMoreValue) {
    this.hasMoreValue = hasMoreValue;
  }

  @Override
  public String next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    return builder.build().url().toString();
  }
}
