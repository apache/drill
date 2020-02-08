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
package org.apache.drill.exec.physical.impl.scan.project.projSet;

import org.apache.drill.exec.physical.resultSet.ProjectionSet;
import org.apache.drill.exec.physical.resultSet.project.RequestedColumn;
import org.apache.drill.exec.physical.resultSet.project.RequestedColumnImpl;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple.TupleProjectionType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;
import org.apache.drill.exec.vector.complex.DictVector;

/**
 * Projection set based on an explicit set of columns provided
 * in the physical plan. Columns in the list are projected, others
 * are not.
 */

public class ExplicitProjectionSet extends AbstractProjectionSet {

  private final RequestedTuple requestedProj;

  public ExplicitProjectionSet(RequestedTuple requestedProj, TypeConverter typeConverter) {
    super(typeConverter);
    this.requestedProj = requestedProj;
  }

  @Override
  public boolean isProjected(String colName) {
    return requestedProj.get(colName) != null;
  }

  @Override
  public ColumnReadProjection readProjection(ColumnMetadata col) {
    RequestedColumn reqCol = requestedProj.get(col.name());
    if (reqCol == null) {
      return new UnprojectedReadColumn(col);
    }

    return getReadProjection(col, reqCol);
  }

  private ColumnReadProjection getReadProjection(ColumnMetadata col, RequestedColumn reqCol) {
    ColumnMetadata outputSchema = outputSchema(col);
    ProjectionChecker.validateProjection(reqCol, outputSchema == null ? col : outputSchema, errorContext);
    if (!col.isMap() && !col.isDict()) {

      // Non-map column.

      ColumnConversionFactory conv = conversion(col, outputSchema);
      return new ProjectedReadColumn(col, reqCol, outputSchema, conv);
    } else {

      // Maps are tuples. Create a tuple projection and wrap it in
      // a column projection.

      TypeConverter childConverter = childConverter(outputSchema);
      ProjectionSet mapProjection;
      if (! reqCol.isTuple() || reqCol.tuple().type() == TupleProjectionType.ALL) {

        // Projection is simple: "m". This is equivalent to
        // (non-SQL) m.*
        // This may also be a projection of the form m.a, m. The
        // general projection takes precedence.

        mapProjection =  new WildcardProjectionSet(childConverter, isStrict);
      } else {

        // Else, selected map items are projected, say m.a, m.c.
        // (Here, we'll never hit the case where none of the map is
        // projected; that case, while allowed in the RequestedTuple
        // implementation, can never occur in a SELECT list.)

        mapProjection = new ExplicitProjectionSet(reqCol.tuple(), childConverter);
      }
      if (col.isMap()) {
        return new ProjectedMapColumn(col, reqCol, outputSchema, mapProjection);
      } else {
        return new ProjectedDictColumn(col, reqCol, outputSchema, mapProjection);
      }
    }
  }

  @Override
  public ColumnReadProjection readDictProjection(ColumnMetadata col) {
    // Unlike for a MAP, requestedProj contains a key value, rather than nested field's name:
    // create DICT's members somewhat artificially

    assert DictVector.fieldNames.contains(col.name());
    if (col.name().equals(DictVector.FIELD_KEY_NAME)) {
      // This field is considered not projected but its
      // vector and writer will be instantiated later.
      return new UnprojectedReadColumn(col);
    }

    RequestedColumn reqCol = new RequestedColumnImpl(requestedProj, col.name()); // this is the 'value' column
    return getReadProjection(col, reqCol);
  }

  @Override
  public boolean isEmpty() { return requestedProj.projections().isEmpty(); }
}
