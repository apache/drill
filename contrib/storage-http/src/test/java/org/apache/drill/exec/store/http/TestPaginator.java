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

import okhttp3.HttpUrl;
import org.apache.drill.exec.store.http.paginator.OffsetPaginator;
import org.apache.drill.exec.store.http.paginator.PagePaginator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * This class tests the functionality of the various paginator classes as it pertains
 * to generating the correct URLS.
 */
public class TestPaginator {

  @Test
  public void TestOffsetPaginator() {
    String baseUrl = "https://myapi.com";
    HttpUrl.Builder urlBuilder = HttpUrl.parse(baseUrl).newBuilder();

    OffsetPaginator op = new OffsetPaginator(urlBuilder, 25, 5, "limit", "offset");
    List<HttpUrl> urls = op.buildPaginatedURLs();
    assertEquals(5, urls.size());

    List<HttpUrl> expected = new ArrayList<>();
    expected.add( HttpUrl.parse("https://myapi.com/?offset=0&limit=5"));
    expected.add( HttpUrl.parse("https://myapi.com/?offset=5&limit=5"));
    expected.add( HttpUrl.parse("https://myapi.com/?offset=10&limit=5"));
    expected.add( HttpUrl.parse("https://myapi.com/?offset=15&limit=5"));
    expected.add( HttpUrl.parse("https://myapi.com/?offset=20&limit=5"));

    assertEquals(expected, urls);
  }

  @Test
  public void TestOffsetPaginatorIterator() {
    String baseUrl = "https://myapi.com";
    HttpUrl.Builder urlBuilder = HttpUrl.parse(baseUrl).newBuilder();

    OffsetPaginator op = new OffsetPaginator(urlBuilder, 25, 5, "limit", "offset");
    assertEquals(5, op.count());

    String next = op.next();
    assertEquals(next, "https://myapi.com/?offset=0&limit=5");
    next = op.next();
    assertEquals(next, "https://myapi.com/?offset=5&limit=5");
    next = op.next();
    assertEquals(next, "https://myapi.com/?offset=10&limit=5");
    next = op.next();
    assertEquals(next, "https://myapi.com/?offset=15&limit=5");
    next = op.next();
    assertEquals(next, "https://myapi.com/?offset=20&limit=5");
    next = op.next();
    assertNull(next);
  }

  @Test
  public void TestPagePaginatorIterator() {
    String baseUrl = "https://api.github.com/orgs/apache/repos";
    HttpUrl.Builder urlBuilder = HttpUrl.parse(baseUrl).newBuilder();

    PagePaginator pp = new PagePaginator(urlBuilder, 10, 2, "page", "per_page");
    List<HttpUrl> urls = pp.getPaginatedUrls();
    assertEquals(5, urls.size());

    PagePaginator pp2 = new PagePaginator(urlBuilder, 10, 100, "page", "per_page");
    List<HttpUrl> urls2 = pp2.getPaginatedUrls();
    assertEquals(1, urls2.size());
  }
}
