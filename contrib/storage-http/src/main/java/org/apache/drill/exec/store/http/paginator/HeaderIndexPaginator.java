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

import okhttp3.Headers;
import okhttp3.HttpUrl.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.http.HttpPaginatorConfig.PaginatorMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Header Index Paginator is used when the API in question send a link in the HTTP header
 * containing the URL for the next page.
 */
public class HeaderIndexPaginator extends Paginator {

  private static final Logger logger = LoggerFactory.getLogger(HeaderIndexPaginator.class);
  private static final Pattern URL_REGEX = Pattern.compile("(https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*))");

  private final String nextPageParam;
  private final String firstPageURL;
  private Headers headers;
  private String nextUrl;
  private boolean firstPage;
  private int pageCount;

  public HeaderIndexPaginator(Builder builder, int pageSize, int limit, String nextPageParam, String firstPageURL) {
    super(builder, PaginatorMethod.HEADER_INDEX, pageSize, limit);
    this.nextPageParam = nextPageParam;
    this.firstPageURL = firstPageURL;
    this.firstPage = true;
    this.pageCount = 0;
  }

  @Override
  public boolean hasNext() {
    // If the headers are null and it isn't the first page, end pagination
    if ( !firstPage &&
        (headers == null || StringUtils.isEmpty(headers.get(nextPageParam)))
    ) {
      notifyPartialPage();
      logger.debug("Ending pagination.  No additional info in headers.");
      return false;
    }

    return !partialPageReceived;
  }

  /**
   * This method sets the headers for the Header Index Paginator.  This must be called with updated headers
   * before the {@link #next()} method is called.
   * @param headers A {@link Headers} object containing the response headers from the previous API call.
   */
  public void setResponseHeaders(Headers headers) {
    logger.debug("Setting response headers. ");
    this.headers = headers;
  }

  @Override
  public String next() {
    pageCount++;
    if (firstPage) {
      firstPage = false;
      return firstPageURL;
    }

    if (headers == null) {
      throw UserException.dataReadError()
          .message("Headers are empty.  HeaderIndex Pagination requires parameters that are passed in the HTTP header." + pageCount)
          .build(logger);
    }
    // Now attempt to retrieve the field from the response headers.
    String nextPage = headers.get(nextPageParam);

    // If the next page value is null or empty, halt pagination
    if (StringUtils.isEmpty(nextPage)) {
      super.notifyPartialPage();
      return null;
    }

    logger.debug("Found next page URL: {}", nextPage);

    // Clean up any extraneous garbage from the header field.
    Matcher urlMatcher = URL_REGEX.matcher(nextPage);
    if (urlMatcher.find()) {
      return urlMatcher.group(1);
    }

    return nextPage;
  }
}
