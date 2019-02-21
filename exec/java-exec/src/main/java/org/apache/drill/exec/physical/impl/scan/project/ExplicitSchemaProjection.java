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
package org.apache.drill.exec.physical.impl.scan.project;

import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple;
import org.apache.drill.exec.physical.rowSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;

/**
 * Perform a schema projection for the case of an explicit list of
 * projected columns. Example: SELECT a, b, c.
 * <p>
 * An explicit projection starts with the requested set of columns,
 * then looks in the table schema to find matches. That is, it is
 * driven by the query itself.
 * <p>
 * An explicit projection may include columns that do not exist in
 * the source schema. In this case, we fill in null columns for
 * unmatched projections.
 */

public class ExplicitSchemaProjection extends SchemaLevelProjection {

  public ExplicitSchemaProjection(ScanLevelProjection scanProj,
      TupleMetadata tableSchema,
      ResolvedTuple rootTuple,
      List<SchemaProjectionResolver> resolvers) {
    super(resolvers);
    resolveRootTuple(scanProj, rootTuple, tableSchema);
  }

  private void resolveRootTuple(ScanLevelProjection scanProj,
      ResolvedTuple rootTuple,
      TupleMetadata tableSchema) {
    for (ColumnProjection col : scanProj.columns()) {
      if (col.nodeType() == UnresolvedColumn.UNRESOLVED) {
        resolveColumn(rootTuple, ((UnresolvedColumn) col).element(), tableSchema);
      } else {
        resolveSpecial(rootTuple, col, tableSchema);
      }
    }
  }

  private void resolveColumn(ResolvedTuple outputTuple,
      RequestedColumn inputCol, TupleMetadata tableSchema) {
    int tableColIndex = tableSchema.index(inputCol.name());
    if (tableColIndex == -1) {
      resolveNullColumn(outputTuple, inputCol);
    } else {
      resolveTableColumn(outputTuple, inputCol,
          tableSchema.metadata(tableColIndex),
          tableColIndex);
    }
  }

  private void resolveTableColumn(ResolvedTuple outputTuple,
      RequestedColumn requestedCol,
      ColumnMetadata column, int sourceIndex) {

    // Is the requested column implied to be a map?
    // A requested column is a map if the user requests x.y and we
    // are resolving column x. The presence of y as a member implies
    // that x is a map.

    if (requestedCol.isTuple()) {
      resolveMap(outputTuple, requestedCol, column, sourceIndex);
    }

    // Is the requested column implied to be an array?
    // This occurs when the projection list contains at least one
    // array index reference such as x[10].

    else if (requestedCol.isArray()) {
      resolveArray(outputTuple, requestedCol, column, sourceIndex);
    }

    // A plain old column. Might be an array or a map, but if
    // so, the request list just mentions it by name without implying
    // the column type. That is, the project list just contains x
    // by itself.

    else {
      projectTableColumn(outputTuple, requestedCol, column, sourceIndex);
    }
  }

  private void resolveMap(ResolvedTuple outputTuple,
      RequestedColumn requestedCol, ColumnMetadata column,
      int sourceIndex) {

    // If the actual column isn't a map, then the request is invalid.

    if (! column.isMap()) {
      throw UserException
        .validationError()
        .message("Project list implies a map column, but actual column is not a map")
        .addContext("Projected column", requestedCol.fullName())
        .addContext("Actual type", column.type().name())
        .build(logger);
    }

    // The requested column is implied to be a map because it lists
    // members to project. Project these.

    ResolvedMapColumn mapCol = new ResolvedMapColumn(outputTuple,
        column.schema(), sourceIndex);
    resolveTuple(mapCol.members(), requestedCol.mapProjection(),
        column.mapSchema());

    // If the projection is simple, then just project the map column
    // as is. A projection is simple if all map columns from the table
    // are projected, and no null columns are needed. The simple case
    // occurs more often than one might expect because the result set
    // loader only projected those columns that were needed, so the only
    // issue we have to handle is null columns.
    //
    // In the simple case, we discard the map tuple just created
    // since we ended up not needing it.

    if (mapCol.members().isSimpleProjection()) {
      outputTuple.removeChild(mapCol.members());
      projectTableColumn(outputTuple, requestedCol, column, sourceIndex);
    }

    // The resolved tuple may have a subset of table columns
    // and/or null columns. Project a new map that will be created
    // to hold the projected map elements.

    else {
      outputTuple.add(mapCol);
    }
  }

  private void resolveTuple(ResolvedTuple mapTuple,
      RequestedTuple requestedTuple, TupleMetadata mapSchema) {
    for (RequestedColumn col : requestedTuple.projections()) {
      resolveColumn(mapTuple, col, mapSchema);
    }
  }

  private void resolveArray(ResolvedTuple outputTuple,
      RequestedColumn requestedCol, ColumnMetadata column,
      int sourceIndex) {

    // If the actual column isn't a array or list,
    // then the request is invalid.

    if (column.type() != MinorType.LIST && ! column.isArray()) {
      throw UserException
        .validationError()
        .message("Project list implies an array, but actual column is not an array")
        .addContext("Projected column", requestedCol.fullName())
        .addContext("Actual cardinality", column.mode().name())
        .build(logger);
    }

    // The project operator will do the actual array element
    // projection.

    projectTableColumn(outputTuple, requestedCol, column, sourceIndex);
  }

  /**
   * Project a column to the specified output tuple. The name comes from the
   * project list. (If the actual column name is `X` (upper case), but the
   * project list requests `x` (lower case), project the column using the
   * lower-case name. The column type comes from the table column. The source
   * index is the location in the table map or row.
   *
   * @param outputTuple
   *          projected tuple being built
   * @param requestedCol
   *          column as requested in the project list
   * @param column
   *          metadata for the actual table column
   * @param sourceIndex
   *          index of the column within the table tuple (implies the location
   *          of the table vector to be projected)
   */

  private void projectTableColumn(ResolvedTuple outputTuple,
      RequestedColumn requestedCol,
      ColumnMetadata column, int sourceIndex) {
     outputTuple.add(
        new ResolvedTableColumn(requestedCol.name(),
            MaterializedField.create(requestedCol.name(),
                column.majorType()),
            outputTuple, sourceIndex));
  }

  /**
   * Resolve a null column. This is a projected column which does not match
   * an implicit or table column. We consider two cases: a simple top-level
   * column reference ("a", say) and an implied map reference ("a.b", say.)
   * If the column appears to be a map, determine the set of children, which
   * map appear to any depth, that were requested.
   */

  private void resolveNullColumn(ResolvedTuple outputTuple,
      RequestedColumn requestedCol) {
    ResolvedColumn nullCol;
    if (requestedCol.isTuple()) {
      nullCol = resolveMapMembers(outputTuple, requestedCol);
    } else {
      nullCol = outputTuple.nullBuilder.add(requestedCol.name());
    }
    outputTuple.add(nullCol);
  }

  /**
   * A child column of a map is not projected. Recurse to determine the full
   * set of nullable child columns.
   *
   * @param projectedColumn the map column which was projected
   * @return a list of null markers for the requested children
   */

  private ResolvedColumn resolveMapMembers(ResolvedTuple outputTuple, RequestedColumn col) {
    ResolvedMapColumn mapCol = new ResolvedMapColumn(outputTuple, col.name());
    ResolvedTuple members = mapCol.members();
    for (RequestedColumn child : col.mapProjection().projections()) {
      if (child.isTuple()) {
        members.add(resolveMapMembers(members, child));
      } else {
        members.add(outputTuple.nullBuilder.add(child.name()));
      }
    }
    return mapCol;
  }
}