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

import java.util.Collection;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.resultSet.ProjectionSet;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple.TupleProjectionType;
import org.apache.drill.exec.physical.resultSet.project.RequestedTupleImpl;
import org.apache.drill.exec.record.metadata.TupleMetadata;

public class ProjectionSetBuilder {

  private RequestedTuple parsedProjection;
  private TypeConverter typeConverter;
  private CustomErrorContext errorContext;

  /**
   * Record (batch) readers often read a subset of available table columns,
   * but want to use a writer schema that includes all columns for ease of
   * writing. (For example, a CSV reader must read all columns, even if the user
   * wants a subset. The unwanted columns are simply discarded.)
   * <p>
   * This option provides a projection list, in the form of column names, for
   * those columns which are to be projected. Only those columns will be
   * backed by value vectors; non-projected columns will be backed by "null"
   * writers that discard all values.
   *
   * @param projection the list of projected columns
   * @return this builder
   */

  public ProjectionSetBuilder projectionList(Collection<SchemaPath> projection) {
    if (projection == null) {
      parsedProjection = null;
    } else {
      parsedProjection = RequestedTupleImpl.parse(projection);
    }
    return this;
  }

  public ProjectionSetBuilder parsedProjection(RequestedTuple projection) {
    parsedProjection = projection;
    return this;
  }

  public ProjectionSetBuilder outputSchema(TupleMetadata schema) {
    typeConverter = TypeConverter.builder().providedSchema(schema).build();
    return this;
  }

  public ProjectionSetBuilder typeConverter(TypeConverter converter) {
    this.typeConverter = converter;
    return this;
  }

  public ProjectionSetBuilder errorContext(CustomErrorContext errorContext) {
    this.errorContext = errorContext;
    return this;
  }

  public ProjectionSet build() {
    TupleProjectionType projType = parsedProjection == null ?
        TupleProjectionType.ALL : parsedProjection.type();

    ProjectionSet projSet;
    switch (projType) {
    case ALL:
      projSet = new WildcardProjectionSet(typeConverter);
      break;
    case NONE:
      projSet = ProjectionSetFactory.projectNone();
      break;
    case SOME:
      projSet = new ExplicitProjectionSet(parsedProjection, typeConverter);
      break;
    default:
      throw new IllegalStateException("Unexpected projection type: " + projType.toString());
    }
    projSet.setErrorContext(errorContext);
    return projSet;
  }
}
