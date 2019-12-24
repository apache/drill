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

package org.apache.drill.exec.store.elasticsearch;

import org.elasticsearch.hadoop.rest.RestService.PartitionDefinition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ElasticSearchScanSpec {
  private final String indexName;

  private final String typeMappingName;

  private PartitionDefinition partitionDefinition;

  private String filters;

  private String queryFilter;

  @JsonCreator
  public ElasticSearchScanSpec(@JsonProperty("indexName") String indexName, @JsonProperty("typeMappingName") String typeMappingName) {
    this.indexName = indexName;
    this.typeMappingName = typeMappingName;

  }

  @JsonCreator
  public ElasticSearchScanSpec(String indexName, String typeMappingName, PartitionDefinition partitionDefinition) {
    this.indexName = indexName;
    this.typeMappingName = typeMappingName;
    this.partitionDefinition = partitionDefinition;
  }


  @JsonCreator
  public ElasticSearchScanSpec(String indexName, String typeMappingName, PartitionDefinition partitionDefinition, String queryFilter) {
    this.indexName = indexName;
    this.typeMappingName = typeMappingName;
    this.partitionDefinition = partitionDefinition;
    this.queryFilter = queryFilter;
  }

  public String getIndexName() {
    return this.indexName;
  }

  public String getTypeMappingName() {
    return typeMappingName;
  }

  public String getFilters() {
    return filters;
  }

  public void setFilters(String filters) {
    this.filters = filters;
  }

  public PartitionDefinition getPartitionDefinition() {
    return partitionDefinition;
  }

  public void setPartitionDefinition(PartitionDefinition partitionDefinition) {
    this.partitionDefinition = partitionDefinition;
  }

  @Override
  public String toString() {
    return "ElasticSearchScanSpec [index=" + this.indexName + ", typeMapping=" + this.typeMappingName + "]";
  }
}
