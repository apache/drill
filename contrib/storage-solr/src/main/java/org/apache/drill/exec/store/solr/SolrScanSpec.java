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

import java.util.List;
import com.google.common.collect.Lists;
import org.apache.drill.exec.store.solr.schema.SolrSchemaPojo;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SolrScanSpec {
  private String solrCoreName;
  private boolean isAggregateQuery = false;
  private boolean isGroup = false;
  private boolean isDataQuery = false;
  private SolrFilterParam filter = new SolrFilterParam();
  private List<SolrAggrParam> aggrParams = Lists.newArrayList();
  private Integer solrDocFetchCount = -1;
  private SolrSchemaPojo cvSchema;
  private List<String> responseFieldList;

  @JsonCreator
  public SolrScanSpec(@JsonProperty("solrCoreName") String solrCoreName) {
    this.solrCoreName = solrCoreName;
  }

  @JsonCreator
  public SolrScanSpec(@JsonProperty("solrCoreName") String solrCoreName,
      @JsonProperty("filter") String filter) {
    this.solrCoreName = solrCoreName;
  }

  @JsonCreator
  public SolrScanSpec(@JsonProperty("solrCoreName") String solrCoreName,
      @JsonProperty("solrCoreSchema") SolrSchemaPojo cvSchema) {
    this.solrCoreName = solrCoreName;
    this.cvSchema = cvSchema;
  }

  @JsonCreator
  public SolrScanSpec(@JsonProperty("solrCoreName") String solrCoreName,
      @JsonProperty("solrDocFetchCount") Integer solrDocFetchCount) {
    this.solrCoreName = solrCoreName;
    this.solrDocFetchCount = solrDocFetchCount;
  }

  public SolrScanSpec(@JsonProperty("solrCoreName") String solrCoreName,
      @JsonProperty("filter") SolrFilterParam filter) {
    this.solrCoreName = solrCoreName;
    this.filter = filter;
  }

  public String getSolrCoreName() {
    return solrCoreName;
  }

  public SolrFilterParam getFilter() {
    return filter;
  }

  public SolrSchemaPojo getCvSchema() {
    return cvSchema;
  }

  public Integer getSolrDocFetchCount() {
    return solrDocFetchCount;
  }

  public boolean isAggregateQuery() {
    return isAggregateQuery;
  }

  public void setAggregateQuery(boolean isAggregateQuery) {
    this.isAggregateQuery = isAggregateQuery;
  }

  public void setFilter(SolrFilterParam filter) {
    this.filter = filter;
  }

  public void setSolrDocFetchCount(Integer solrDocFetchCount) {
    this.solrDocFetchCount = solrDocFetchCount;
  }

  public void setCvSchema(SolrSchemaPojo cvSchema) {
    this.cvSchema = cvSchema;
  }

  public List<SolrAggrParam> getAggrParams() {
    return aggrParams;
  }

  public void setAggrParams(List<SolrAggrParam> aggrParams) {
    this.aggrParams = aggrParams;
  }

  public List<String> getResponseFieldList() {
    return responseFieldList;
  }

  public void setResponseFieldList(List<String> responseFieldList) {
    this.responseFieldList = responseFieldList;
  }

  public boolean isDataQuery() {
    return isDataQuery;
  }

  public void setDataQuery(boolean isDataQuery) {
    this.isDataQuery = isDataQuery;
  }

  public boolean isGroup() {
    return isGroup;
  }

  public void setGroup(boolean isGroup) {
    this.isGroup = isGroup;
  }

  @Override
  public String toString() {
    return "SolrScanSpec [solrCoreName=" + solrCoreName + ", filter=" + filter
        + ", solrDocFetchCount=" + solrDocFetchCount + " aggreegation="
        + aggrParams + "]";
  }
}
