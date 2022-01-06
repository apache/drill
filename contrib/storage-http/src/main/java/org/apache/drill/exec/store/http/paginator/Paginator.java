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

import okhttp3.HttpUrl;
import okhttp3.HttpUrl.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This class is the abstraction for the Paginator class.  There are
 * different pagination methods, however, all essentially require the query
 * engine to generate URLs to retrieve the next batch of data and also
 * to determine whether the URL has more data.
 *
 * The Offset and Page paginators work either with a limit or without, but function
 * slightly differently.  If a limit is specified and pushed down, the paginator will
 * generate a list of URLs with the appropriate pagination parameters.  In the future
 * this could be parallelized, however in the V1 all these requests are run in series.
 */
public abstract class Paginator {

  private static final Logger logger = LoggerFactory.getLogger(Paginator.class);
  private static final int MAX_ATTEMPTS = 100;
  protected final int pageSize;
  private boolean hasMore;

  public enum paginationMode {
    OFFSET,
    PAGE
  }

  protected final boolean hasLimit;
  protected final paginationMode MODE;
  protected int index;
  protected Builder builder;
  protected List<HttpUrl> paginatedUrls;


  public Paginator(Builder builder, paginationMode mode, int pageSize, boolean hasLimit) {
    this.MODE = mode;
    this.builder = builder;
    this.pageSize = pageSize;
    this.hasLimit = hasLimit;
    hasMore = true;
    index = 0;
  }

  public void setBuilder(Builder builder) {
    this.builder = builder;
  }

  public abstract List<HttpUrl> buildPaginatedURLs();

  /**
   * This method is to be used when the user does not include a limit in the query
   * In each paginator, the paginator tracks whether there is more data.  If there is
   * more data, the paginator marks the hasMore variable false.
   * @return
   */
  public abstract String generateNextUrl();

  public List<HttpUrl> getPaginatedUrls() {
    return this.paginatedUrls;
  }

  /**
   * This method is used in pagination queries when no limit is present.  The intended
   * flow is that if no limit is present, if the batch reader encounters a page which has
   * less data than the page size, the batch reader should call this method to stop
   * the Paginator from generating additional URLs to call.
   *
   * In the event that the API simply runs out of data, the reader will return false anyway
   * and the pagination will stop.
   */
  public void endPagination() {
    hasMore = false;
  }

  public int getPageSize() { return pageSize; }

  public String next() {
    if (!hasMore()) {
      return null;
    }
    String url = paginatedUrls.get(index).toString();
    index++;
    if (index >= paginatedUrls.size()) {
      hasMore = false;
    }
    return url;
  }

  /**
   * Returns true if the paginator has more pages, false if not.
   * @return True if there are more pages to visit, false if not.
   */
  public boolean hasMore() {
    return hasMore;
  }

  /**
   * Returns the count of URLs generated.  Only meaningful for OFFSET paginator.
   * @return The count of pages.
   */
  public int count() {
    if (paginatedUrls == null) {
      return 0;
    } else {
      return paginatedUrls.size();
    }
  }
}
