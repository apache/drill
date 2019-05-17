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
package org.apache.drill.exec.physical.rowSet;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.exec.physical.rowSet.project.ProjectionType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;
import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;

/**
 * Provides a dynamic, run-time view of a projection set. Used by
 * the result set loader to:
 * <ul>
 * <li>Determine if a column is projected according to some
 * defined projection schema (see implementation for details.)</li>
 * <li>Provide type conversions, either using built-in implicit
 * conversions, or a custom conversion. Type conversions require
 * the reader column and a "provided" column that gives the "to"
 * type for the conversion. Without the "to" column, the reader
 * column type is used as-is.</li>
 * <li>Verify that the (possibly converted) type and mode are
 * compatible with an explicit projection item. For example, if
 * the query has `a.b`, but `a` is scalar, then there is an
 * inconsistency.</li>
 * </ul>
 * <p>
 * This interface filters columns added dynamically
 * at scan time. The reader may offer a column (as to add a column
 * writer for the column.) The projection mechanism says whether to
 * materialize the column, or whether to ignore the column and
 * return a dummy column writer.
 * <p>
 * The Project All must handle several additional nuances:
 * <ul>
 * <li>External schema: If an external schema is provided, then that
 * schema may be "strict" which causes the wildcard to expand to the
 * set of columns defined within the schema. When used with columns
 * added dynamically, a column may be excluded from the projection
 * set if it is not part of the defined external schema.</ul>
 * <li>Metadata filtering: A reader may offer a special column which
 * is available only in explicit projection, and behaves like Drill's
 * implicit file columns. Such columns are not included in a "project
 * all" projection.</li>
 * <p>
 * At present, only the top-level row supports these additional filtering
 * options; they are not supported on maps (though could be with additional
 * effort.)
 * <p>
 * Special columns are generic and thus handled here. External schema
 * is handled in a subclass in the scan projection framework.
 * <p>
 */
public interface ProjectionSet {

  /**
   * Response to a query against a reader projection to indicate projection
   * status of a reader-provided column. This is a transient object which
   * indicates whether a reader column is projected, and if so, the attributes
   * of that projection.
   */

  public interface ColumnReadProjection {

    /**
     * Determine if the given column is to be projected. Used when
     * adding columns to the result set loader. Skips columns omitted
     * from an explicit projection, or columns within a wildcard projection
     * where the column is "special" and is not expanded in the wildcard.
     */

    boolean isProjected();

    ColumnMetadata readSchema();
    ColumnMetadata providedSchema();
    ColumnConversionFactory conversionFactory();
    ProjectionSet mapProjection();

    /**
     * The projection type from the parse of the projection list,
     * if available. Used for testing only. Don't use this in production
     * code, let this class do the checks itself.
     */
    @VisibleForTesting
    ProjectionType projectionType();
  }

  void setErrorContext(CustomErrorContext errorContext);
  ColumnReadProjection readProjection(ColumnMetadata col);
}
