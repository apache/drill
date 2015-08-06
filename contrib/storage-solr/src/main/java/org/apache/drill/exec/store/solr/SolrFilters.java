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

import org.apache.drill.exec.store.solr.SolrScanSpec.SolrFilterParam;

public class SolrFilters {
  private SolrFilterParam leftFilterParam;
  private String operator;
  private SolrFilterParam rightFilterParam;

  public SolrFilters(SolrFilterParam leftFilterParam, String operator,
      SolrFilterParam rightFilterParam) {
    super();
    this.leftFilterParam = leftFilterParam;
    this.operator = operator;
    this.rightFilterParam = rightFilterParam;
  }

  public SolrFilters() {
    super();
  }

  public SolrFilterParam getLeftFilterParam() {
    return leftFilterParam;
  }

  public void setLeftFilterParam(SolrFilterParam leftFilterParam) {
    this.leftFilterParam = leftFilterParam;
  }

  public String getOperator() {
    return operator;
  }

  public void setOperator(String operator) {
    this.operator = operator;
  }

  public SolrFilterParam getRightFilterParam() {
    return rightFilterParam;
  }

  public void setRightFilterParam(SolrFilterParam rightFilterParam) {
    this.rightFilterParam = rightFilterParam;
  }
}
