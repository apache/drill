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
package org.apache.drill.exec.planner.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.EnumSet;

/**
 * A visitor that is very similar to {@link FindLimit0SqlVisitor} in that it looks for a LIMIT 0
 * in the root portion of the query tree for the sake of enabling optimisations but that is
 * different in the following aspects.
 *
 *  1. This visitor enables the
 *  {@link org.apache.drill.exec.ExecConstants#FILE_LISTING_LIMIT0_OPT} optimisation which takes
 *  effect during query validation, before the query has been converted to a RelNode tree.
 *  2. This visitor is less thorough about discovering usable LIMIT 0 nodes because of the
 *  preceding point. For example, it does not even try to make use of LIMIT 0s that are present
 *  in CTEs (see SqlKind.WITH_ITEM below). Since the real targets of LIMIT 0 optimisations are
 *  schema probing queries which almost always have their LIMIT 0 on the outermost SELECT, this
 *  visitor should nevertheless do the job sufficiently.
 *  3. This visitor is interested in whether any scanned input data is needed for the query's
 *  results, rather than whether there are zero results e.g. aggregates like SUM return a single
 *  (null) result even when they have zero inputs.
 *
 */
public class FindLimit0SqlVisitor extends SqlShuttle {
  private static final EnumSet<SqlKind> SEARCH_TERMINATING_NODES = EnumSet.of(
      SqlKind.JOIN,
      SqlKind.UNION,
      SqlKind.EXCEPT,
      SqlKind.WITH_ITEM
  );

  private boolean rootContainsLimit0;

  /**
   * Do a non-exhaustive check of whether the root portion of the SQL node tree contains LIMIT(0)
   *
   * @param sql SQL node tree
   * @return true if the root portion of the tree contains LIMIT(0)
   */
  public static boolean containsLimit0(final SqlNode sql) {
    FindLimit0SqlVisitor visitor = new FindLimit0SqlVisitor();
    sql.accept(visitor);
    return visitor.rootContainsLimit0;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    SqlKind kind = call.getKind();
    if (SEARCH_TERMINATING_NODES.contains(kind)) {
      return call;
    }
    if (kind == SqlKind.ORDER_BY) {
      SqlOrderBy orderBy = (SqlOrderBy) call;
      SqlNumericLiteral limitLiteral = (SqlNumericLiteral) orderBy.fetch;

      if (limitLiteral != null && limitLiteral.longValue(true) == 0) {
        rootContainsLimit0 = true;
        return call;
      }
    }
    // Continue down the tree.
    return super.visit(call);
  }
}
