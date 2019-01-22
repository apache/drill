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
package org.apache.drill.exec.physical.impl.scan.framework;

import java.util.List;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.scan.ScanOperatorEvents;
import org.apache.drill.exec.physical.impl.scan.project.ScanSchemaOrchestrator;

/**
 * Basic scan framework for a "managed" reader which uses the scan schema
 * mechanisms encapsulated in the scan schema orchestrator. Handles binding
 * scan events to the scan orchestrator so that the scan schema is evolved
 * as the scan progresses. Subclasses are responsible for creating the actual
 * reader, which requires a framework-specific schema negotiator to be passed
 * to the reader.
 * <p>
 * This framework is a bridge between operator logic and the scan projection
 * internals. It gathers scan-specific options, then sets
 * then on the scan orchestrator at the right time. By abstracting out this
 * plumbing, a scan batch creator simply chooses the proper framework, passes
 * config options, and implements the matching "managed reader". All details
 * of setup, projection, and so on are handled by the framework and the components
 * that the framework builds upon.
 */

public abstract class AbstractScanFramework<T extends SchemaNegotiator> implements ScanOperatorEvents {

  protected final List<SchemaPath> projection;
  protected MajorType nullType;
  protected int maxBatchRowCount;
  protected int maxBatchByteCount;
  protected boolean v1_12MetadataLocation;
  protected OperatorContext context;
  protected ScanSchemaOrchestrator scanOrchestrator;

  public AbstractScanFramework(List<SchemaPath> projection) {
    this.projection = projection;
  }

  /**
   * Specify the type to use for projected columns that do not
   * match any data source columns. Defaults to nullable int.
   */

  public void setNullType(MajorType type) {
    this.nullType = type;
  }

  public void setMaxRowCount(int rowCount) {
    maxBatchRowCount = rowCount;
  }

  public void setMaxBatchByteCount(int byteCount) {
    maxBatchByteCount = byteCount;
  }

  /**
   * In Drill 1.11, and Drill 1.13, metadata columns come after the data
   * columns. In Drill 1.11, metadata columns come before the data columns.
   * This flag preserves the 1.12 behavior for testing purposes.
   *
   * @param flag true to use the Drill 1.12 metadata position, false to
   * use the Drill 1.1-1.11 & Drill 1.13 position
   */

  public void useDrill1_12MetadataPosition(boolean flag) {
    v1_12MetadataLocation = flag;
  }

  @Override
  public void bind(OperatorContext context) {
    this.context = context;
    scanOrchestrator = new ScanSchemaOrchestrator(context.getAllocator());
    configure();
    assert projection != null;
    scanOrchestrator.build(projection);
  }

  public OperatorContext context() { return context; }

  public ScanSchemaOrchestrator scanOrchestrator() {
    return scanOrchestrator;
  }

  protected void configure() {

    // Pass along config options if set.

    if (maxBatchRowCount > 0) {
      scanOrchestrator.setBatchRecordLimit(maxBatchRowCount);
    }
    if (maxBatchByteCount > 0) {
      scanOrchestrator.setBatchByteLimit(maxBatchByteCount);
    }
    if (nullType != null) {
      scanOrchestrator.setNullType(nullType);
    }
    scanOrchestrator.useDrill1_12MetadataPosition(v1_12MetadataLocation);
  }

  public abstract boolean openReader(ShimBatchReader<T> shim, ManagedReader<T> reader);

  @Override
  public void close() {
    if (scanOrchestrator != null) {
      scanOrchestrator.close();
      scanOrchestrator = null;
    }
  }
}
