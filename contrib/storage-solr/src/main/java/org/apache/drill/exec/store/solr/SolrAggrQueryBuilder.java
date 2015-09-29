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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrAggrQueryBuilder extends
    AbstractExprVisitor<SolrScanSpec, Void, RuntimeException> {

  static final Logger logger = LoggerFactory.getLogger(SolrQueryBuilder.class);
  final SolrGroupScan groupScan;
  final LogicalExpression le;
  private boolean allExpressionsConverted = true;

  public SolrAggrQueryBuilder(SolrGroupScan solrGroupScan,
      LogicalExpression conditionExp) {
    this.groupScan = solrGroupScan;
    this.le = conditionExp;
    logger.debug("SolrAggrQueryBuilder :: constructor");
  }

  public SolrScanSpec parseTree() {
    logger.debug("SolrQueryBuilder :: parseTree");
    SolrScanSpec parsedSpec = le.accept(this, null);

    return parsedSpec;
  }

  @Override
  public SolrScanSpec visitUnknown(LogicalExpression e, Void valueArg)
      throws RuntimeException {
    logger.info("SolrQueryBuilder :: visitUnknown");
    allExpressionsConverted = false;
    return null;
  }

  public boolean isAllExpressionsConverted() {
    return allExpressionsConverted;
  }

  @Override
  public SolrScanSpec visitFunctionHolderExpression(
      FunctionHolderExpression fhe, Void valueArg) {
    logger.info("SolrQueryBuilder :: visitFunctionHolderExpression");

    return null;

  }

  @Override
  public SolrScanSpec visitBooleanOperator(BooleanOperator op, Void valueArg) {
    return null;
  }

  @Override
  public SolrScanSpec visitFunctionCall(FunctionCall call, Void valueArg)
      throws RuntimeException {
    logger.debug("SolrQueryBuilder :: visitFunctionCall");
    SolrAggrFunctionProcessor evaluator = SolrAggrFunctionProcessor
        .process(call);
    if (evaluator.isSuccess() && evaluator.getPath() != null) {
      SolrAggrParam solrAggrParam = new SolrAggrParam();
      solrAggrParam.setFieldName(evaluator.getPath().toString()
          .replaceAll("`", ""));
      solrAggrParam.setFunctionName(evaluator.getFunctionName());
      List<SolrAggrParam> aggrParams = this.groupScan.getSolrScanSpec()
          .getAggrParams() != null ? this.groupScan.getSolrScanSpec()
          .getAggrParams() : new ArrayList<SolrAggrParam>();
      aggrParams.add(solrAggrParam);

      SolrScanSpec solrScanSpec = this.groupScan.getSolrScanSpec();
      solrScanSpec.setAggregateQuery(true);
      solrScanSpec.setAggrParams(aggrParams);

      logger.debug(" evaluator 1 " + evaluator.getFunctionName());
      logger.debug(" evaluator 2 " + evaluator.getValue());
      logger.debug(" evaluator 3 " + evaluator.getPath());
      return solrScanSpec;
    }
    return null;
  }
}
