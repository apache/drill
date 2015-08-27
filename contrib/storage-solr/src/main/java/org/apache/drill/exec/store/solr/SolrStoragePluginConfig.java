/**
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
package org.apache.drill.exec.store.solr;

import org.apache.drill.common.logical.StoragePluginConfig;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(SolrStoragePluginConfig.NAME)
public class SolrStoragePluginConfig extends StoragePluginConfig {
  public static final String NAME = "solr";
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(SolrStoragePluginConfig.class);

  @JsonProperty
  private String solrServer = "";

  @JsonCreator
  public SolrStoragePluginConfig(@JsonProperty("solrServer") String solrServer) {
    logger
        .debug("Initializing SOLR StoragePlugin configuration with solr server :: "
            + solrServer);
    this.solrServer = solrServer;

  }

  @JsonProperty("solrServer")
  public String getSolrServer() {
    return this.solrServer;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that) {
      return true;
    } else if (that == null || getClass() != that.getClass()) {
      return false;
    }
    SolrStoragePluginConfig thatConfig = (SolrStoragePluginConfig) that;
    return this.solrServer.equals(thatConfig.solrServer);

  }

  @Override
  public int hashCode() {
    return this.solrServer != null ? this.solrServer.hashCode() : 0;
  }
}
