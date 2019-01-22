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
package org.apache.drill.exec.physical.impl.scan.columns;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.project.ColumnProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionParser;
import org.apache.drill.exec.physical.impl.scan.project.UnresolvedColumn;
import org.apache.drill.exec.physical.rowSet.project.RequestedColumnImpl;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.store.easy.text.compliant.RepeatedVarCharOutput;

/**
 * Parses the `columns` array. Doing so is surprisingly complex.
 * <ul>
 * <li>Depending on what is known about the input file, the `columns`
 * array may be required or optional.</li>
 * <li>If the columns array is required, then the wildcard (`*`)
 * expands to `columns`.</li>
 * <li>If the columns array appears, then no other table columns
 * can appear.</li>
 * <li>If the columns array appears, then the wildcard cannot also
 * appear, unless that wildcard expanded to be `columns` as
 * described above.</li>
 * <li>The query can select specific elements such as `columns`[2].
 * In this case, only array elements can appear, not the unindexed
 * `columns` column.</li>
 * </ul>
 * <p>
 * It falls to this parser to detect a not-uncommon user error, a
 * query such as the following:<pre><code>
 * SELECT max(columns[1]) AS col1
 * FROM cp.`textinput/input1.csv`
 * WHERE col1 IS NOT NULL
 * </code></pre>
 * In standard SQL, column aliases are not allowed in the WHERE
 * clause. So, Drill will push two columns down to the scan operator:
 * `columns`[1] and `col1`. This parser will detect the "extra"
 * columns and must provide a message that helps the user identify
 * the likely original problem.
 */

public class ColumnsArrayParser implements ScanProjectionParser {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ColumnsArrayParser.class);

  // Config

  private final boolean requireColumnsArray;

  // Internals

  private ScanLevelProjection builder;

  // Output

  private UnresolvedColumnsArrayColumn columnsArrayCol;

  public ColumnsArrayParser(boolean requireColumnsArray) {
    this.requireColumnsArray = requireColumnsArray;
  }

  @Override
  public void bind(ScanLevelProjection builder) {
    this.builder = builder;
  }

  @Override
  public boolean parse(RequestedColumn inCol) {
    if (requireColumnsArray && inCol.isWildcard()) {
      expandWildcard();
      return true;
    }
    if (! inCol.nameEquals(ColumnsArrayManager.COLUMNS_COL)) {
      return false;
    }

    // The columns column cannot be a map. That is, the following is
    // not allowed: columns.foo.

    if (inCol.isTuple()) {
      throw UserException
        .validationError()
        .message("{} has map elements, but cannot be a map", inCol.name())
        .build(logger);
    }

    if (inCol.isArray()) {
      int maxIndex = inCol.maxIndex();
      if (maxIndex > RepeatedVarCharOutput.MAXIMUM_NUMBER_COLUMNS) {
        throw UserException
          .validationError()
          .message(String.format(
              "`columns`[%d] index out of bounds, max supported size is %d",
              maxIndex, RepeatedVarCharOutput.MAXIMUM_NUMBER_COLUMNS))
          .addContext("Column", inCol.name())
          .addContext("Maximum index", RepeatedVarCharOutput.MAXIMUM_NUMBER_COLUMNS)
          .build(logger);
      }
    }

    // Special `columns` array column.

    columnsArrayCol = new UnresolvedColumnsArrayColumn(inCol);
    builder.addTableColumn(columnsArrayCol);
    return true;
  }

  /**
   * Query contained SELECT *, and we know that the reader supports only
   * the `columns` array; go ahead and expand the wildcard to the only
   * possible column.
   */

  private void expandWildcard() {
    if (columnsArrayCol != null) {
      throw UserException
        .validationError()
        .message("Cannot select columns[] and `*` together")
        .build(logger);
    }
    columnsArrayCol = new UnresolvedColumnsArrayColumn(
        new RequestedColumnImpl(builder.rootProjection(), ColumnsArrayManager.COLUMNS_COL));
    builder.addTableColumn(columnsArrayCol);
  }

  @Override
  public void validate() {
    if (builder.hasWildcard() && columnsArrayCol != null) {
      throw UserException
        .validationError()
        .message("Cannot select `columns` and `*` together")
        .build(logger);
    }
  }

  @Override
  public void validateColumn(ColumnProjection col) {
    if (col.nodeType() == UnresolvedColumn.UNRESOLVED) {
      if (columnsArrayCol != null) {
        throw UserException
          .validationError()
          .message("Cannot select columns[] and other table columns. Column alias incorrectly used in the WHERE clause?")
          .addContext("Column name", col.name())
          .build(logger);
      }
      if (requireColumnsArray) {
        throw UserException
          .validationError()
          .message("Only `columns` column is allowed. Found: " + col.name())
          .build(logger);
      }
    }
  }

  @Override
  public void build() { }

  public UnresolvedColumnsArrayColumn columnsArrayCol() { return columnsArrayCol; }
}
