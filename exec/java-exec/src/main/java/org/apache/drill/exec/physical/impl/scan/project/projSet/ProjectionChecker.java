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

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.physical.resultSet.project.RequestedColumn;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProjectionChecker {
  private static final Logger logger = LoggerFactory.getLogger(ProjectionChecker.class);

  private ProjectionChecker() { }

  public static boolean isConsistent(RequestedTuple tuple, ColumnMetadata readCol) {
    if (tuple == null || !tuple.isProjected(readCol.name())) {
      return true;
    }
    // If the column is projected, it may be projected implicitly.
    // Only check explicit projection.
    RequestedColumn col = tuple.get(readCol.name());
    if (col == null) {
      return true;
    } else {
      return isConsistent(col, readCol);
    }
  }

  /**
   * Does a superficial check of projection type against the given column.
   * Does not handle subtleties such as DICT key types, actual types
   * in a UNION, etc.
   */
  public static boolean isConsistent(RequestedColumn colReq, ColumnMetadata readCol) {
    if (colReq == null || readCol == null) {
      return true;
    }
    if (colReq.isTuple() && !(readCol.isMap() || readCol.isDict() || readCol.isVariant())) {
      return false;
    }
    if (colReq.isArray()) {
      if (colReq.arrayDims() == 1) {
        return readCol.isArray() || readCol.isDict() || readCol.isVariant();
      } else {
        return readCol.type() == MinorType.LIST || readCol.isDict() || readCol.isVariant();
      }
    }
    return true;
  }

  public static void validateProjection(RequestedColumn colReq, ColumnMetadata readCol) {
    validateProjection(colReq, readCol, null);
  }

  public static void validateProjection(RequestedColumn colReq, ColumnMetadata readCol,
      CustomErrorContext errorContext) {
    if (!isConsistent(colReq, readCol)) {
      throw UserException.validationError()
        .message("Column type not compatible with projection specification")
        .addContext("Column:", readCol.name())
        .addContext("Projection type:", colReq.toString())
        .addContext("Column type:", Types.getSqlTypeName(readCol.majorType()))
        .addContext(errorContext)
        .build(logger);
    }
  }
}
