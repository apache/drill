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
package org.apache.drill.exec.store.cassandra;


import java.util.List;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CassandraFilterBuilder extends
        AbstractExprVisitor<CassandraScanSpec, Void, RuntimeException> implements
        DrillCassandraConstants {
    static final Logger logger = LoggerFactory.getLogger(CassandraFilterBuilder.class);
    final CassandraGroupScan groupScan;
    final LogicalExpression le;
    private boolean allExpressionsConverted = true;

    public CassandraFilterBuilder(CassandraGroupScan groupScan,
                              LogicalExpression conditionExp) {
        this.groupScan = groupScan;
        this.le = conditionExp;

    }

    /* Called by CassandraPushDownFilterForScan */
    public CassandraScanSpec parseTree() {
        CassandraScanSpec parsedSpec = le.accept(this, null);

        if (parsedSpec != null) {
            parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getCassandraScanSpec(), parsedSpec);
        }
        return parsedSpec;
    }

    private CassandraScanSpec mergeScanSpecs(String functionName,
                                             CassandraScanSpec leftScanSpec, CassandraScanSpec rightScanSpec) {

    List<Clause> newFilters = Lists.newArrayList();
    CassandraScanSpec scanSpec = null;
    switch (functionName) {
        case "booleanAnd":
            if (leftScanSpec != null && leftScanSpec.getFilters() != null) {
                newFilters.addAll(leftScanSpec.getFilters());
            }
            if (rightScanSpec != null && rightScanSpec.getFilters() != null) {
                newFilters.addAll(rightScanSpec.getFilters());
            }

            scanSpec = new CassandraScanSpec(groupScan.getCassandraScanSpec().getKeyspace(),
                    groupScan.getCassandraScanSpec().getTable(), newFilters);
            break;
    }

    return scanSpec;
    }

    public boolean isAllExpressionsConverted() {
        return allExpressionsConverted;
    }

    @Override
    public CassandraScanSpec visitUnknown(LogicalExpression e, Void value)
            throws RuntimeException {
        allExpressionsConverted = false;
        return null;
    }

    @Override
    public CassandraScanSpec visitBooleanOperator(BooleanOperator op, Void value) {

        List<LogicalExpression> args = op.args;
        CassandraScanSpec nodeScanSpec = null;
        String functionName = op.getName();
        for (int i = 0; i < args.size(); ++i) {
            switch (functionName) {
                case "booleanAnd":
                    if (nodeScanSpec == null) {
                        nodeScanSpec = args.get(i).accept(this, null);
                    } else {
                        CassandraScanSpec scanSpec = args.get(i).accept(this, null);
                        if (scanSpec != null) {
                            nodeScanSpec = mergeScanSpecs(functionName, nodeScanSpec, scanSpec);
                        } else {
                            allExpressionsConverted = false;
                        }
                    }
                    break;
            }
        }
        return nodeScanSpec;
    }

    @Override
    public CassandraScanSpec visitFunctionCall(FunctionCall call, Void value)
            throws RuntimeException {

        CassandraScanSpec nodeScanSpec = null;
        String functionName = call.getName();
        ImmutableList<LogicalExpression> args = call.args;

        if (CassandraCompareFunctionsProcessor.isCompareFunction(functionName)) {
            CassandraCompareFunctionsProcessor processor = CassandraCompareFunctionsProcessor.process(call);
            if (processor.isSuccess()) {
                nodeScanSpec = createCassandraScanSpec(call, processor);
            }
        } else {
            switch (functionName) {
                case "booleanAnd":
                    CassandraScanSpec firstScanSpec = args.get(0).accept(this, null);
                    for (int i = 1; i < args.size(); ++i) {
                        CassandraScanSpec nextScanSpec = args.get(i).accept(this, null);
                        if (firstScanSpec != null && nextScanSpec != null) {
                            nodeScanSpec = mergeScanSpecs(functionName, firstScanSpec, nextScanSpec);
                        } else {
                            allExpressionsConverted = false;
                            if ("booleanAnd".equals(functionName)) {
                                nodeScanSpec = firstScanSpec == null ? nextScanSpec : firstScanSpec;
                            }
                        }
                        firstScanSpec = nodeScanSpec;
                    }
                    break;
            }
        }

        if (nodeScanSpec == null) {
            allExpressionsConverted = false;
        }

        return nodeScanSpec;
    }



    private CassandraScanSpec createCassandraScanSpec(FunctionCall call, CassandraCompareFunctionsProcessor processor) {
        String functionName = processor.getFunctionName();
        SchemaPath field = processor.getPath();
        String fieldValue = processor.getValue();

        Clause clause = null;
        boolean isNullTest = false;
        switch (functionName) {
            case "equal":
                clause = QueryBuilder.eq(field.getAsNamePart().getName(), fieldValue);
                break;
            case "greater_than_or_equal_to":
                clause = QueryBuilder.gte(field.getAsNamePart().getName(), fieldValue);
                break;
            case "greater_than":
                clause = QueryBuilder.gt(field.getAsNamePart().getName(), fieldValue);
                break;
            case "less_than_or_equal_to":
                clause = QueryBuilder.lte(field.getAsNamePart().getName(), fieldValue);
                break;
            case "less_than":
                clause = QueryBuilder.lt(field.getAsNamePart().getName(), fieldValue);
                break;
            case "isnull":
            case "isNull":
            case "is null":
                clause = QueryBuilder.eq(field.getAsNamePart().getName(), null);
                break;
        }

        if (clause != null) {
            List<Clause> filters = Lists.newArrayList();
            filters.add(clause);
            return new CassandraScanSpec(groupScan.getCassandraScanSpec().getKeyspace(),
                    groupScan.getCassandraScanSpec().getTable(), filters);
        }
        // else
        return null;
    }
}
