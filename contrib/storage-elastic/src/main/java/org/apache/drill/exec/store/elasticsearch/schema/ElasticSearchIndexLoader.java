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

package org.apache.drill.exec.store.elasticsearch.schema;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.elasticsearch.ElasticSearchConstants;

import com.google.common.cache.CacheLoader;

import org.apache.drill.exec.store.elasticsearch.ElasticSearchStoragePlugin;
import org.apache.drill.exec.store.elasticsearch.JsonHelper;
import org.elasticsearch.client.Response;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class ElasticSearchIndexLoader extends CacheLoader<String, Collection<String>> {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticSearchIndexLoader.class);

  private final ElasticSearchStoragePlugin plugin;

  public ElasticSearchIndexLoader(ElasticSearchStoragePlugin plugin) {
    this.plugin = plugin;
  }

  @Override
  public Collection<String> load(String key) throws Exception {
    if (!ElasticSearchConstants.INDEXES.equals(key)) {
      throw new UnsupportedOperationException();
    }
    Set<String> indexes = Sets.newHashSet();
    try {
      // 拉取所有的表回来
      Response response = this.plugin.getClient().performRequest("GET", "/_aliases");
      JsonNode jsonNode = JsonHelper.readRespondeContentAsJsonTree(this.plugin.getObjectMapper(), response);
      Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        JsonNode aliases = JsonHelper.getPath(entry.getValue(), "aliases");
        // 2.3.3 版本拉取回来的索引是这种状态 ObjectNode 为空的
        if (!aliases.isMissingNode() && !(aliases instanceof ObjectNode)) {
          Iterator<String> aliasesIterator = aliases.fieldNames();
          while (aliasesIterator.hasNext()) {
            indexes.add(aliasesIterator.next());
          }
        } else {
          // 所有的索引数据
          indexes.add(entry.getKey());
        }
      }
      return indexes;
    } catch (RuntimeException me) {
      logger.warn("Failure while loading indexes from ElasticSearch. {}", me.getMessage());
      return Collections.emptyList();
    } catch (Exception e) {
      throw new DrillRuntimeException(e.getMessage(), e);
    }
  }
}
