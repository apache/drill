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
package org.apache.drill.exec.planner.sql.parser;

import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlVisitor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Implementation of {@link SqlVisitor} that converts bracketed compound {@link SqlIdentifier} to bracket-less compound
 * {@link SqlIdentifier} (also known as {@link DrillCompoundIdentifier}) to provide ease of use while querying complex
 * types.
 * <p/>
 * For example, this visitor converts {@code a['b'][4]['c']} to {@code a.b[4].c}
 */
public class CompoundIdentifierConverter extends SqlShuttle {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CompoundIdentifierConverter.class);

  private boolean enableComplex = true;

  @Override
  public SqlNode visit(SqlIdentifier id) {
    if(id instanceof DrillCompoundIdentifier){
      if(enableComplex){
        return ((DrillCompoundIdentifier) id).getAsSqlNode();
      }else{
        return ((DrillCompoundIdentifier) id).getAsCompoundIdentifier();
      }

    }else{
      return id;
    }
  }

  @Override
  public SqlNode visit(final SqlCall call) {
    // Handler creates a new copy of 'call' only if one or more operands
    // change.
    ArgHandler<SqlNode> argHandler = new ComplexExpressionAware(call);
    call.getOperator().acceptCall(this, call, false, argHandler);
    return argHandler.result();
  }


  private class ComplexExpressionAware implements ArgHandler<SqlNode>  {
    boolean update;
    SqlNode[] clonedOperands;
    RewriteType[] rewriteTypes;
    private final SqlCall call;

    public ComplexExpressionAware(SqlCall call) {
      this.call = call;
      this.update = false;
      final List<SqlNode> operands = call.getOperandList();
      this.clonedOperands = operands.toArray(new SqlNode[operands.size()]);
      rewriteTypes = REWRITE_RULES.get(call.getClass());
    }

    @Override
    public SqlNode result() {
      if (update) {
        return call.getOperator().createCall(
            call.getFunctionQuantifier(),
            call.getParserPosition(),
            clonedOperands);
      } else {
        return call;
      }
    }

    @Override
    public SqlNode visitChild(
        SqlVisitor<SqlNode> visitor,
        SqlNode expr,
        int i,
        SqlNode operand) {
      if (operand == null) {
        return null;
      }

      boolean localEnableComplex = enableComplex;
      if(rewriteTypes != null){
        switch(rewriteTypes[i]){
        case DISABLE:
          enableComplex = false;
          break;
        case ENABLE:
          enableComplex = true;
        }
      }
      SqlNode newOperand = operand.accept(CompoundIdentifierConverter.this);
      enableComplex = localEnableComplex;
      if (newOperand != operand) {
        update = true;
      }
      clonedOperands[i] = newOperand;
      return newOperand;
    }
  }

  static final Map<Class<? extends SqlCall>, RewriteType[]> REWRITE_RULES;

  enum RewriteType {
    UNCHANGED, DISABLE, ENABLE;
  }

  static {
    final RewriteType E =RewriteType.ENABLE;
    final RewriteType D =RewriteType.DISABLE;
    final RewriteType U =RewriteType.UNCHANGED;

    /*
    This map stores the rules that instruct each SqlCall class which data field needs
    to be rewritten if that data field is a CompoundIdentifier

    Key  : Each rule corresponds to a SqlCall class;
    value: It is an array of RewriteType, each being associated with a data field
           in that class.

           For example, there are four data fields (query, orderList, offset, fetch)
           in org.eigenbase.sql.SqlOrderBy. Since only orderList needs to be written,
           RewriteType[] should be R(D, E, D, D).
    */
    Map<Class<? extends SqlCall>, RewriteType[]> rules = Maps.newHashMap();

  //SqlNodeList keywordList,
  //SqlNodeList selectList,
  //SqlNode fromClause,
  //SqlNode whereClause,
  //SqlNodeList groupBy,
  //SqlNode having,
  //SqlNodeList windowDecls,
  //SqlNodeList orderBy,
  //SqlNode offset,
  //SqlNode fetch,
    rules.put(SqlSelect.class, R(D, E, D, E, E, E, E, E, D, D));
    rules.put(SqlCreateTable.class, R(D, D, D, E));
    rules.put(SqlCreateView.class, R(D, E, E, D));
    rules.put(SqlDescribeTable.class, R(D, D, E));
    rules.put(SqlDropView.class, R(D, D));
    rules.put(SqlShowFiles.class, R(D));
    rules.put(SqlShowSchemas.class, R(D, D));
    rules.put(SqlUseSchema.class, R(D));
    rules.put(SqlJoin.class, R(D, D, D, D, D, E));
    rules.put(SqlOrderBy.class, R(D, E, D, D));
    rules.put(SqlDropTable.class, R(D, D));
    rules.put(SqlRefreshMetadata.class, R(D));
    rules.put(SqlSetOption.class, R(D, D, D));
    rules.put(SqlDescribeSchema.class, R(D));
    REWRITE_RULES = ImmutableMap.copyOf(rules);
  }

  // Each type in the input arguments refers to
  // each data field in the class
  private static RewriteType[] R(RewriteType... types){
    return types;
  }

}
