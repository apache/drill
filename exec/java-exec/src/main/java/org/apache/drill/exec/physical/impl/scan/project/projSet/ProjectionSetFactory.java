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

import java.util.List;
import java.util.Map;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.project.projSet.TypeConverter.CustomTypeTransform;
import org.apache.drill.exec.physical.resultSet.ProjectionSet;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple;
import org.apache.drill.exec.physical.resultSet.project.RequestedTupleImpl;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions.ConversionDefn;

public class ProjectionSetFactory {

  private static class SimpleTransform implements CustomTypeTransform {

    private final ColumnConversionFactory colFactory;

    public SimpleTransform(ColumnConversionFactory colFactory) {
      this.colFactory = colFactory;
    }

    @Override
    public ColumnConversionFactory transform(ColumnMetadata inputDefn,
        Map<String, String> properties,
        ColumnMetadata outputDefn, ConversionDefn defn) {
      return colFactory;
    }
  }

  public static ProjectionSet projectAll() { return new WildcardProjectionSet(null); }

  public static ProjectionSet projectNone() { return EmptyProjectionSet.PROJECT_NONE; }


  public static ProjectionSet wrap(RequestedTuple mapProjection) {
    switch (mapProjection.type()) {
    case ALL:
      return projectAll();
    case NONE:
      return projectNone();
    case SOME:
      return new ExplicitProjectionSet(mapProjection, null);
    default:
      throw new IllegalStateException("Unexpected projection type: " +
            mapProjection.type().toString());
    }
  }

  public static ProjectionSet build(List<SchemaPath> selection) {
    if (selection == null) {
      return projectAll();
    }
    return wrap(RequestedTupleImpl.parse(selection));
  }

  public static CustomTypeTransform simpleTransform(ColumnConversionFactory colFactory) {
    return new SimpleTransform(colFactory);
  }
}
