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
package org.apache.drill.exec.store.parquet;

import com.google.common.collect.ImmutableList;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;

import java.io.IOException;
import java.util.List;

public class ParquetFilterBuilder extends
        AbstractExprVisitor<FilterPredicate, Void, RuntimeException> {
    static final Logger logger = LoggerFactory
            .getLogger(ParquetFilterBuilder.class);
    private LogicalExpression le;
    private boolean allExpressionsConverted = true;
    private ParquetGroupScan groupScan;

    public ParquetFilterBuilder(ParquetGroupScan groupScan, LogicalExpression conditionExp) {
        this.le = conditionExp;
        this.groupScan = groupScan;
    }

    public ParquetGroupScan parseTree() {
        FilterPredicate predicate = le.accept(this, null);
        try {
            return this.groupScan.clone(predicate);
        } catch (IOException e) {
            logger.error("Failed to set Parquet filter", e);
            return null;
        }
    }

    public boolean areAllExpressionsConverted() {
        return allExpressionsConverted;
    }

    @Override
    public FilterPredicate visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
        allExpressionsConverted = false;
        return null;
    }

    @Override
    public FilterPredicate visitBooleanOperator(BooleanOperator op, Void value) {
        List<LogicalExpression> args = op.args;
        FilterPredicate nodePredicate = null;
        String functionName = op.getName();
        for (LogicalExpression arg : args) {
            switch (functionName) {
                case "booleanAnd":
                case "booleanOr":
                    if (nodePredicate == null) {
                        nodePredicate = arg.accept(this, null);
                    } else {
                        FilterPredicate predicate = arg.accept(this, null);
                        if (predicate != null) {
                            nodePredicate = mergePredicates(functionName, nodePredicate, predicate);
                        } else {
                            // we can't include any part of the OR if any of the predicates cannot be converted
                            if (functionName == "booleanOr") {
                                nodePredicate = null;
                            }
                            allExpressionsConverted = false;
                        }
                    }
                    break;
            }
        }
        return nodePredicate;
    }

    private FilterPredicate mergePredicates(String functionName,
                                            FilterPredicate leftPredicate, FilterPredicate rightPredicate) {
        if (leftPredicate != null && rightPredicate != null) {
            if (functionName == "booleanAnd") {
                return FilterApi.and(leftPredicate, rightPredicate);
            }
            else {
                return FilterApi.or(leftPredicate, rightPredicate);
            }
        } else {
            allExpressionsConverted = false;
            if ("booleanAnd".equals(functionName)) {
                return leftPredicate == null ? rightPredicate : leftPredicate;
            }
        }

        return null;
    }

    @Override
    public FilterPredicate visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
        FilterPredicate predicate = null;
        String functionName = call.getName();
        ImmutableList<LogicalExpression> args = call.args;

        if (ParquetCompareFunctionProcessor.isCompareFunction(functionName)) {
            ParquetCompareFunctionProcessor processor = ParquetCompareFunctionProcessor
                    .process(call);
            if (processor.isSuccess()) {
                try {
                    predicate = createFilterPredicate(processor.getFunctionName(),
                            processor.getPath(), processor.getValue());
                } catch (Exception e) {
                    logger.error("Failed to create Parquet filter", e);
                }
            }
        } else {
            switch (functionName) {
                case "booleanAnd":
                case "booleanOr":
                    FilterPredicate leftPredicate = args.get(0).accept(this, null);
                    FilterPredicate rightPredicate = args.get(1).accept(this, null);
                    predicate = mergePredicates(functionName, leftPredicate, rightPredicate);
                    break;
            }
        }

        if (predicate == null) {
            allExpressionsConverted = false;
        }

        return predicate;
    }

    private FilterPredicate createFilterPredicate(String functionName,
                                                  SchemaPath field, Object fieldValue) {
        FilterPredicate filter = null;

        // extract the field name
        String fieldName = field.getAsUnescapedPath();
        switch (functionName) {
            case "equal":
                if (fieldValue instanceof Long) {
                    filter = FilterApi.eq(FilterApi.longColumn(fieldName), (Long) fieldValue);
                }
                else if (fieldValue instanceof Integer) {
                    filter = FilterApi.eq(FilterApi.intColumn(fieldName), (Integer) fieldValue);
                }
                else if (fieldValue instanceof Float) {
                    filter = FilterApi.eq(FilterApi.floatColumn(fieldName), (Float) fieldValue);
                }
                else if (fieldValue instanceof Double) {
                    filter = FilterApi.eq(FilterApi.doubleColumn(fieldName), (Double) fieldValue);
                }
                else if (fieldValue instanceof Boolean) {
                    filter = FilterApi.eq(FilterApi.booleanColumn(fieldName), (Boolean) fieldValue);
                }
                break;
            case "not_equal":
                if (fieldValue instanceof Long) {
                    filter = FilterApi.notEq(FilterApi.longColumn(fieldName), (Long) fieldValue);
                }
                else if (fieldValue instanceof Integer) {
                    filter = FilterApi.notEq(FilterApi.intColumn(fieldName), (Integer) fieldValue);
                }
                else if (fieldValue instanceof Float) {
                    filter = FilterApi.notEq(FilterApi.floatColumn(fieldName), (Float) fieldValue);
                }
                else if (fieldValue instanceof Double) {
                    filter = FilterApi.notEq(FilterApi.doubleColumn(fieldName), (Double) fieldValue);
                }
                else if (fieldValue instanceof Boolean) {
                    filter = FilterApi.notEq(FilterApi.booleanColumn(fieldName), (Boolean) fieldValue);
                }
                break;
            case "greater_than_or_equal_to":
                if (fieldValue instanceof Long) {
                    filter = FilterApi.gtEq(FilterApi.longColumn(fieldName), (Long) fieldValue);
                }
                else if (fieldValue instanceof Integer) {
                    filter = FilterApi.gtEq(FilterApi.intColumn(fieldName), (Integer) fieldValue);
                }
                else if (fieldValue instanceof Float) {
                    filter = FilterApi.gtEq(FilterApi.floatColumn(fieldName), (Float) fieldValue);
                }
                else if (fieldValue instanceof Double) {
                    filter = FilterApi.gtEq(FilterApi.doubleColumn(fieldName), (Double) fieldValue);
                }
                break;
            case "greater_than":
                if (fieldValue instanceof Long) {
                    filter = FilterApi.gt(FilterApi.longColumn(fieldName), (Long) fieldValue);
                }
                else if (fieldValue instanceof Integer) {
                    filter = FilterApi.gt(FilterApi.intColumn(fieldName), (Integer) fieldValue);
                }
                else if (fieldValue instanceof Float) {
                    filter = FilterApi.gt(FilterApi.floatColumn(fieldName), (Float) fieldValue);
                }
                else if (fieldValue instanceof Double) {
                    filter = FilterApi.gt(FilterApi.doubleColumn(fieldName), (Double) fieldValue);
                }
                break;
            case "less_than_or_equal_to":
                if (fieldValue instanceof Long) {
                    filter = FilterApi.ltEq(FilterApi.longColumn(fieldName), (Long) fieldValue);
                }
                else if (fieldValue instanceof Integer) {
                    filter = FilterApi.ltEq(FilterApi.intColumn(fieldName), (Integer) fieldValue);
                }
                else if (fieldValue instanceof Float) {
                    filter = FilterApi.ltEq(FilterApi.floatColumn(fieldName), (Float) fieldValue);
                }
                else if (fieldValue instanceof Double) {
                    filter = FilterApi.ltEq(FilterApi.doubleColumn(fieldName), (Double) fieldValue);
                }
                break;
            case "less_than":
                if (fieldValue instanceof Long) {
                    filter = FilterApi.lt(FilterApi.longColumn(fieldName), (Long) fieldValue);
                }
                else if (fieldValue instanceof Integer) {
                    filter = FilterApi.lt(FilterApi.intColumn(fieldName), (Integer) fieldValue);
                }
                else if (fieldValue instanceof Float) {
                    filter = FilterApi.lt(FilterApi.floatColumn(fieldName), (Float) fieldValue);
                }
                else if (fieldValue instanceof Double) {
                    filter = FilterApi.lt(FilterApi.doubleColumn(fieldName), (Double) fieldValue);
                }
                break;
            case "isnull":
            case "isNull":
            case "is null":
                if (fieldValue instanceof Long) {
                    filter = FilterApi.eq(FilterApi.longColumn(fieldName), null);
                }
                else if (fieldValue instanceof Integer) {
                    filter = FilterApi.eq(FilterApi.intColumn(fieldName), null);
                }
                else if (fieldValue instanceof Float) {
                    filter = FilterApi.eq(FilterApi.floatColumn(fieldName), null);
                }
                else if (fieldValue instanceof Double) {
                    filter = FilterApi.eq(FilterApi.doubleColumn(fieldName), null);
                }
                break;
            case "isnotnull":
            case "isNotNull":
            case "is not null":
                if (fieldValue instanceof Long) {
                    filter = FilterApi.notEq(FilterApi.longColumn(fieldName), null);
                }
                else if (fieldValue instanceof Integer) {
                    filter = FilterApi.notEq(FilterApi.intColumn(fieldName), null);
                }
                else if (fieldValue instanceof Float) {
                    filter = FilterApi.notEq(FilterApi.floatColumn(fieldName), null);
                }
                else if (fieldValue instanceof Double) {
                    filter = FilterApi.notEq(FilterApi.doubleColumn(fieldName), null);
                }
                break;
        }

        return filter;
    }
}
