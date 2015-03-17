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

package org.apache.drill.exec.physical.impl.aggregate;

import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.drill.exec.planner.StarColumnHelper;
import org.apache.drill.exec.record.BatchSchema;

import com.google.common.collect.Lists;

import java.util.List;

public class AggregateUtils {
  /**
   * Given the grouping keys and the incomingSchema,
   * if the key is * or prefixed * (i.e., T# || *), expand it according to the incomingSchema and add to the output list;
   * if it is not, just add it to the output list
   * @param keys grouping keys, which could be * (or prefixed *)
   * @param incomingSchema BatchSchema of the incoming record batch
   * @return the expanded list of grouping keys
   */
  public static List<NamedExpression> expandGroupByColumns(final NamedExpression[] keys, final BatchSchema incomingSchema) {
    final List<NamedExpression> groupedCols = Lists.newArrayList();
    for(int i = 0; keys != null && i < keys.length; ++i) {
      final SchemaPath expr = (SchemaPath) keys[i].getExpr();
      final String exprPath = expr.getRootSegment().getPath();
      // If the sorting column has *
      if(exprPath.contains(StarColumnHelper.STAR_COLUMN)) {
        final boolean exprHasPrefix = exprPath.contains(StarColumnHelper.PREFIX_DELIMITER);

        // If the sorting column is T# || *
        if(exprHasPrefix)  {
          final String prefix = exprPath.substring(0, exprPath.indexOf(StarColumnHelper.PREFIX_DELIMITER));

          for(int indexCol = 0; indexCol < incomingSchema.getFieldCount(); ++indexCol) {
            final SchemaPath incomingPath = incomingSchema.getColumn(indexCol).getPath();
            if(incomingPath.getRootSegment().getPath().startsWith(prefix)) {
              final NamedExpression ne = new NamedExpression(incomingPath,
                  new FieldReference(incomingPath));
              groupedCols.add(ne);
            }
          }
        // If the sorting column has *
        } else {
          for(int indexCol = 0; indexCol < incomingSchema.getFieldCount(); ++indexCol) {
            final SchemaPath incomingPath = incomingSchema.getColumn(indexCol).getPath();
            final NamedExpression ne = new NamedExpression(incomingPath,
                new FieldReference(incomingPath));
            groupedCols.add(ne);
          }
        }
      } else {
        groupedCols.add(keys[i]);
      }
    }

    return groupedCols;
  }
}
