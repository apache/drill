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
package org.apache.drill.exec.physical.resultSet.impl;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.resultSet.project.Projections;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple.TupleProjectionType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Projection filter used when adding columns to the result set loader.
 * Provides a variety of ways to filter columns: no filtering, filter
 * by (parsed) projection list, or filter by projection list and
 * provided schema. Enforces consistency of actual reader schema and
 * projection list and/or provided schema.
 * <p>
 * Projection filters should not need to be extensible; filtering
 * depends only on projection and provided schema.
 */
public interface ProjectionFilter {
  Logger logger = LoggerFactory.getLogger(ProjectionFilter.class);

  ProjectionFilter PROJECT_ALL = new ImplicitProjectionFilter(true);
  ProjectionFilter PROJECT_NONE = new ImplicitProjectionFilter(false);

  boolean isProjected(String colName);

  boolean isProjected(ColumnMetadata columnSchema);

  ProjectionFilter mapProjection(boolean isColProjected, String colName);

  boolean isEmpty();

  public static ProjectionFilter filterFor(RequestedTuple tupleProj,
      CustomErrorContext errorContext) {
    if (tupleProj.type() == TupleProjectionType.ALL) {
      return PROJECT_ALL;
    } else {
      return new DirectProjectionFilter(tupleProj, errorContext);
    }
  }

  public static ProjectionFilter filterFor(RequestedTuple tupleProj,
      TupleMetadata providedSchema, CustomErrorContext errorContext) {
    if (providedSchema == null) {
      return filterFor(tupleProj, errorContext);
    }
    return new CompoundProjectionFilter(
        new DirectProjectionFilter(tupleProj, errorContext),
        new SchemaProjectionFilter(providedSchema, errorContext));
  }

  /**
   * Implied projection: either project all or project none. Never
   * projects special columns (those marked as not being expanded in
   * SELECT *).
   */
  public static class ImplicitProjectionFilter implements ProjectionFilter {
    private final boolean projectAll;

    public ImplicitProjectionFilter(boolean projectAll) {
      this.projectAll = projectAll;
    }

    @Override
    public boolean isProjected(String name) {
      return projectAll;
    }

    @Override
    public boolean isProjected(ColumnMetadata columnSchema) {
      return projectAll ? !Projections.excludeFromWildcard(columnSchema) : false;
    }

    @Override
    public ProjectionFilter mapProjection(boolean isColProjected, String colName) {
      return isColProjected ? this : PROJECT_NONE;
    }

    @Override
    public boolean isEmpty() {
      return !projectAll;
    }
  }

  /**
   * Projection filter based on the (parsed) projection list. Enforces that
   * the reader column is consistent with the form of projection (map,
   * array, or plain) in the projection list.
   */
  public static class DirectProjectionFilter implements ProjectionFilter {
    private final RequestedTuple projectionSet;
    private final CustomErrorContext errorContext;

    public DirectProjectionFilter(RequestedTuple projectionSet, CustomErrorContext errorContext) {
      this.projectionSet = projectionSet;
      this.errorContext = errorContext;
    }

    @Override
    public boolean isProjected(String colName) {
      return projectionSet.isProjected(colName);
    }

    @Override
    public boolean isProjected(ColumnMetadata columnSchema) {
      return projectionSet.enforceProjection(columnSchema, errorContext);
    }

    @Override
    public ProjectionFilter mapProjection(boolean isColProjected, String colName) {
      return isColProjected ?
        filterFor(projectionSet.mapProjection(colName), errorContext) :
        PROJECT_NONE;
    }

    @Override
    public boolean isEmpty() {
      return projectionSet.isEmpty();
    }
  }

  /**
   * Projection based on a provided schema. If the schema is strict, a reader column
   * is projected only if that column appears in the provided schema. Non-strict
   * schema allow additional reader columns.
   * <p>
   * If the column is found, enforces that the reader schema has the same type and
   * mode as the provided column.
   */
  public static class SchemaProjectionFilter implements ProjectionFilter {
    private final TupleMetadata providedSchema;
    private final CustomErrorContext errorContext;
    private final boolean isStrict;

    public SchemaProjectionFilter(TupleMetadata providedSchema, CustomErrorContext errorContext) {
      this(providedSchema,
          providedSchema.booleanProperty(TupleMetadata.IS_STRICT_SCHEMA_PROP),
          errorContext);
    }

    private SchemaProjectionFilter(TupleMetadata providedSchema, boolean isStrict, CustomErrorContext errorContext) {
      this.providedSchema = providedSchema;
      this.errorContext = errorContext;
      this.isStrict = isStrict;
    }

    @Override
    public boolean isProjected(String name) {
      ColumnMetadata providedCol = providedSchema.metadata(name);
      return providedCol != null || !isStrict;
    }

    @Override
    public boolean isProjected(ColumnMetadata columnSchema) {
      ColumnMetadata providedCol = providedSchema.metadata(columnSchema.name());
      if (providedCol == null) {
        return !isStrict;
      }
      if (providedCol.type() != columnSchema.type() ||
          providedCol.mode() != columnSchema.mode()) {
        throw UserException.validationError()
          .message("Reader and provided column type mismatch")
          .addContext("Provided column", providedCol.columnString())
          .addContext("Reader column", columnSchema.columnString())
          .addContext(errorContext)
          .build(logger);
      }
      return true;
    }

    @Override
    public ProjectionFilter mapProjection(boolean isColProjected, String colName) {
      if (!isColProjected) {
        return PROJECT_NONE;
      }
      ColumnMetadata providedCol = providedSchema.metadata(colName);
      if (providedCol == null) {
        return PROJECT_ALL;
      }
      if (!providedCol.isMap()) {
        throw UserException.validationError()
          .message("Reader expected a map column, but the the provided column is not a map")
          .addContext("Provided column", providedCol.columnString())
          .addContext("Reader column", colName)
          .addContext(errorContext)
          .build(logger);
      }
      return new SchemaProjectionFilter(providedCol.tupleSchema(), isStrict, errorContext);
    }

    @Override
    public boolean isEmpty() {
       return providedSchema.isEmpty();
    }
  }

  /**
   * Compound filter for combining direct and provided schema projections.
   */
  public static class CompoundProjectionFilter implements ProjectionFilter {
    private final ProjectionFilter filter1;
    private final ProjectionFilter filter2;

    public CompoundProjectionFilter(ProjectionFilter filter1, ProjectionFilter filter2) {
      this.filter1 = filter1;
      this.filter2 = filter2;
    }

    @Override
    public boolean isProjected(String name) {
      return filter1.isProjected(name) && filter2.isProjected(name);
    }

    @Override
    public boolean isProjected(ColumnMetadata columnSchema) {
      return filter1.isProjected(columnSchema) && filter2.isProjected(columnSchema);
    }

    @Override
    public ProjectionFilter mapProjection(boolean isColProjected, String colName) {
      ProjectionFilter childFilter1 = filter1.mapProjection(isColProjected, colName);
      ProjectionFilter childFilter2 = filter2.mapProjection(isColProjected, colName);
      if (childFilter1 == PROJECT_ALL) {
        return childFilter2;
      }
      if (childFilter1 == PROJECT_NONE) {
        return childFilter1;
      }
      if (childFilter2 == PROJECT_ALL) {
        return childFilter1;
      }
      if (childFilter2 == PROJECT_NONE) {
        return childFilter2;
      }
      return new CompoundProjectionFilter(childFilter1, childFilter2);
    }

    @Override
    public boolean isEmpty() {
      return filter1.isEmpty() && filter2.isEmpty();
    }
  }
}
