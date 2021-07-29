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
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.drill.exec.store.http.util.SimpleHttp;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestURLParameters {

  @Test
  public void testUrlParameters() {
    // Http client setup
    HttpUrl githubSingleParam = HttpUrl.parse("https://github.com/orgs/{org}/repos");
    Map<String, String> filters = new CaseInsensitiveMap<>();
    filters.put("org", "apache");
    filters.put("param1", "value1");
    filters.put("param2", "value2");
    assertEquals(SimpleHttp.mapURLParameters(githubSingleParam, filters), "https://github.com/orgs/apache/repos");


    HttpUrl githubMultiParam = HttpUrl.parse("https://github.com/orgs/{org}/{repos}");
    Map<String, String> filters2 = new CaseInsensitiveMap<>();
    filters2.put("org", "apache");
    filters2.put("param1", "value1");
    filters2.put("repos", "drill");
    assertEquals(SimpleHttp.mapURLParameters(githubMultiParam, filters2), "https://github.com/orgs/apache/drill");

    HttpUrl githubNoParam = HttpUrl.parse("https://github.com/orgs/org/repos");
    Map<String, String> filters3 = new CaseInsensitiveMap<>();
    filters3.put("org", "apache");
    filters3.put("param1", "value1");
    filters3.put("repos", "drill");
    assertEquals(SimpleHttp.mapURLParameters(githubNoParam, filters3), "https://github.com/orgs/org/repos");
  }

  @Test
  public void testParamAtEnd() {
    HttpUrl pokemonUrl = HttpUrl.parse("https://pokeapi.co/api/v2/pokemon/{pokemon_name}");
    Map<String, String> filters = new CaseInsensitiveMap<>();
    filters.put("pokemon_name", "Misty");
    filters.put("param1", "value1");
    filters.put("repos", "drill");
    assertEquals(SimpleHttp.mapURLParameters(pokemonUrl, filters), "https://pokeapi.co/api/v2/pokemon/Misty");
  }

  @Test
  public void testUpperCase() {
    HttpUrl githubSingleParam = HttpUrl.parse("https://github.com/orgs/{ORG}/repos");
    Map<String, String> filters = new CaseInsensitiveMap<>();
    filters.put("org", "apache");
    filters.put("param1", "value1");
    filters.put("param2", "value2");
    assertEquals(SimpleHttp.mapURLParameters(githubSingleParam, filters), "https://github.com/orgs/apache/repos");

  }
}
