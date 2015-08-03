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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

public class SolrQueryFilterRule extends StoragePluginOptimizerRule {

  public SolrQueryFilterRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
    // TODO Auto-generated constructor stub
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    // TODO Auto-generated method stub
    
  }

}
