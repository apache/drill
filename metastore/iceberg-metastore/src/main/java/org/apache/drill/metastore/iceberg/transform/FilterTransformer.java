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
package org.apache.drill.metastore.iceberg.transform;

import org.apache.drill.metastore.expressions.FilterExpression;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Transforms given input into Iceberg {@link Expression} which is used as filter
 * to retrieve, overwrite or delete Metastore component data.
 */
public class FilterTransformer {

  private final FilterExpression.Visitor<Expression> visitor = FilterExpressionVisitor.get();

  public Expression transform(FilterExpression filter) {
    return filter == null ? Expressions.alwaysTrue() : filter.accept(visitor);
  }

  public Expression transform(Map<String, Object> conditions) {
    if (conditions == null || conditions.isEmpty()) {
      return Expressions.alwaysTrue();
    }

    List<Expression> expressions = conditions.entrySet().stream()
      .map(entry -> Expressions.equal(entry.getKey(), entry.getValue()))
      .collect(Collectors.toList());

    if (expressions.size() == 1) {
      return expressions.get(0);
    }

    return Expressions.and(expressions.get(0), expressions.get(1),
      expressions.subList(2, expressions.size()).toArray(new Expression[0]));
  }
}
