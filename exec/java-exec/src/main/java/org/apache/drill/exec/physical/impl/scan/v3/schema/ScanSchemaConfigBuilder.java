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
package org.apache.drill.exec.physical.impl.scan.v3.schema;

import java.util.Collections;
import java.util.List;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.EmptyErrorContext;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ScanProjectionParser.ProjectionParseResult;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;

/**
 * Builds the configuration given to the {@link ScanSchemaTracker}.
 */
public class ScanSchemaConfigBuilder {
  private TupleMetadata definedSchema;
  private List<SchemaPath> projectionList;
  private TupleMetadata providedSchema;
  private boolean allowSchemaChange;
  private CustomErrorContext errorContext;

  public ScanSchemaConfigBuilder() {
    projectionList = Collections.singletonList(SchemaPath.STAR_COLUMN);
    allowSchemaChange = true;
  }

  public ScanSchemaConfigBuilder projection(List<SchemaPath> projectionList) {
    this.projectionList = projectionList;
    return this;
  }

  public ScanSchemaConfigBuilder definedSchema(TupleMetadata definedSchema) {
    this.definedSchema = definedSchema;
    return this;
  }

  public ScanSchemaConfigBuilder providedSchema(TupleMetadata providedSchema) {
    this.providedSchema = providedSchema;
    return this;
  }

  public ScanSchemaConfigBuilder allowSchemaChange(boolean flag) {
    this.allowSchemaChange = flag;
    return this;
  }

  public ScanSchemaConfigBuilder errorContext(CustomErrorContext errorContext) {
    this.errorContext = errorContext;
    return this;
  }

  public ScanSchemaTracker build() {
    if (errorContext == null) {
      errorContext = EmptyErrorContext.INSTANCE;
    }
    ProjectionParseResult result;
    if (projectionList == null) {
      result = null;
    } else {
      result = ScanProjectionParser.parse(projectionList);
    }

    if (providedSchema != null && SchemaUtils.isStrict(providedSchema)) {
      allowSchemaChange = false;
    }
    if (definedSchema == null) {
      ProjectionSchemaTracker tracker = new ProjectionSchemaTracker(result, allowSchemaChange, errorContext);
      if (providedSchema != null) {
        tracker.applyProvidedSchema(providedSchema);
      }
      return tracker;
    } else {
      if (!MetadataUtils.hasDynamicColumns(definedSchema)) {
        SchemaBasedTracker tracker = new SchemaBasedTracker(definedSchema, errorContext);
        tracker.validateProjection(result.dynamicSchema);
        return tracker;
      } else {
        return new ProjectionSchemaTracker(definedSchema, result, errorContext);
      }
    }
  }
}
