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
package org.apache.drill.exec.store.paimon;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.paimon.plan.DrillExprToPaimonTranslator;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class for common Paimon read operations such as applying filters
 * and projections to ReadBuilder instances.
 */
public final class PaimonReadUtils {
  private static final Logger logger = LoggerFactory.getLogger(PaimonReadUtils.class);

  private PaimonReadUtils() {
    // Utility class
  }

  /**
   * Applies a filter expression to the Paimon ReadBuilder.
   *
   * @param readBuilder the Paimon ReadBuilder
   * @param rowType the table row type
   * @param condition the filter condition
   */
  public static void applyFilter(ReadBuilder readBuilder, RowType rowType, LogicalExpression condition) {
    if (condition == null) {
      return;
    }
    Predicate predicate = DrillExprToPaimonTranslator.translate(condition, rowType);
    if (predicate != null) {
      readBuilder.withFilter(predicate);
    }
  }

  /**
   * Applies column projection to the Paimon ReadBuilder.
   *
   * @param readBuilder the Paimon ReadBuilder
   * @param rowType the table row type
   * @param columns the columns to project
   */
  public static void applyProjection(ReadBuilder readBuilder, RowType rowType, List<SchemaPath> columns) {
    if (columns == null || columns.isEmpty()) {
      return;
    }

    boolean hasStar = columns.stream().anyMatch(SchemaPath::isDynamicStar);
    if (hasStar) {
      return;
    }

    Set<String> projectedNames = new HashSet<>();
    List<Integer> projection = new ArrayList<>();
    for (SchemaPath column : columns) {
      PathSegment segment = column.getRootSegment();
      if (segment == null || !segment.isNamed()) {
        continue;
      }
      String name = segment.getNameSegment().getPath();
      if (!projectedNames.add(name)) {
        continue;
      }
      int index = rowType.getFieldIndex(name);
      if (index < 0) {
        throw UserException.validationError()
          .message("Paimon column not found: %s", name)
          .build(logger);
      }
      projection.add(index);
    }

    if (!projection.isEmpty()) {
      int[] projectionArray = projection.stream().mapToInt(Integer::intValue).toArray();
      readBuilder.withProjection(projectionArray);
    }
  }
}