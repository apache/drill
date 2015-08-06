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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.params.SolrParams;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SolrScanSpec {
  private String solrCoreName;
  private List<SolrFilters> filter;

  @JsonCreator
  public SolrScanSpec(@JsonProperty("solrCoreName") String solrCoreName) {
    this.solrCoreName = solrCoreName;
  }

  @JsonCreator
  public SolrScanSpec(@JsonProperty("solrCoreName") String solrCoreName,
      @JsonProperty("filter") String filter) {
    this.solrCoreName = solrCoreName;
  }

  public String getSolrCoreName() {
    return solrCoreName;
  }

  public List<SolrFilters> getFilter() {
    return filter;
  }

  class SolrFilterParam {
    private String filterName;
    private String operator;
    private String filterValue;

    public SolrFilterParam(String filterName, String operator,
        String filterValue) {
      this.filterName = filterName;
      this.operator = operator;
      this.filterValue = filterValue;
    }
  }
  @Override
  public String toString(){
    return "SolrScanSpec [solrCoreName=" + solrCoreName + ", filter=" + filter + "]";
  }
}
