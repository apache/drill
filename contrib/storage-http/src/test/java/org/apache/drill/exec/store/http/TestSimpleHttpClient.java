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

import org.apache.drill.exec.store.http.util.SimpleHttp;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestSimpleHttpClient {

  @Test
  public void testUrlParameters() throws Exception {
    // Http client setup
    SimpleHttp http = new SimpleHttp("https://github.com/orgs/{org}/repos");
    Map<String, String> filters = new HashMap<>();
    filters.put("org", "apache");
    filters.put("param1", "value1");
    filters.put("param2", "value2");
    assertEquals(http.mapURLParameters(filters), "https://github.com/orgs/apache/repos");


    SimpleHttp http2 = new SimpleHttp("https://github.com/orgs/{org}/{repos}");
    Map<String, String> filters2 = new HashMap<>();
    filters.put("org", "apache");
    filters.put("param1", "value1");
    filters.put("repos", "drill");
    assertEquals(http2.mapURLParameters(filters), "https://github.com/orgs/apache/drill");
  }
}
