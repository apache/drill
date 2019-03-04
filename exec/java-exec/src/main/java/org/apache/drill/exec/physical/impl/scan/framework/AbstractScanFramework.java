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
 * them on the scan orchestrator at the right time. By abstracting out this
 * plumbing, a scan batch creator simply chooses the proper framework, passes
 * config options, and implements the matching "managed reader". All details
 * of setup, projection, and so on are handled by the framework and the components
 * that the framework builds upon.
 *
 * <h4>Inputs</h4>
 *
 * At this basic level, a scan framework requires just a few simple inputs:
 * <ul>
 * <li>The projection list provided by the physical operator definition. This
 * list identifies the set of "output" columns whih this framework is obliged
 * to produce.</li>
 * <li>The operator context which provides access to a memory allocator and
 * other plumbing items.</li>
 * <li>A method to create a reader for each of the files or blocks
 * defined above. (Readers are created one-by-one as files are read.)</li>
 * <li>The data type to use for projected columns which the reader cannot
 * provide. (Drill allows such columns and fills in null values: traditionally
 * nullable Int, but customizable here.)
 * <li>Various other options.</li>
 * </ul>
 *
 * <h4>Orchestration</h4>
 *
 * The above is sufficient to drive the entire scan operator functionality.
 * Projection is done generically and is the same for all files. Only the
 * reader (created via the factory class) differs from one type of file to
 * another.
 * <p>
 * The framework achieves the work described below= by composing a large
 * set of detailed classes, each of which performs some specific task. This
 * structure leaves the reader to simply infer schema and read data.
 * <p>
 * In particular, rather than do all the orchestration here (which would tie
 * that logic to the scan operation), the detailed work is delegated to the
 * {@link ScanSchemaOrchestrator} class, with this class as a "shim" between
 * the the Scan events API and the schema orchestrator implementation.
 *
 * <h4>Reader Integration</h4>
 *
 * The details of how a file is structured, how a schema is inferred, how
 * data is decoded: all that is encapsulated in the reader. The only real
 * Interaction between the reader and the framework is:
 * <ul>
 * <li>The reader "negotiates" a schema with the framework. The framework
 * knows the projection list from the query plan, knows something about
 * data types (whether a column should be scalar, a map or an array), and
 * knows about the schema already defined by prior readers. The reader knows
 * what schema it can produce (if "early schema.") The schema negotiator
 * class handles this task.</li>
 * <li>The reader reads data from the file and populates value vectors a
 * batch at a time. The framework creates the result set loader to use for
 * this work. The schema negotiator returns that loader to the reader, which
 * uses it during read.
 * <p>
 * It is important to note that the result set loader also defines a schema:
 * the schema requested by the reader. If the reader wants to read three
 * columns, a, b, and c, then that is the schema that the result set loader
 * supports. This is true even if the query plan only wants column a, or
 * wants columns c, a. The framework handles the projection task so the
 * reader does not have to worry about it. Reading an unwanted column
 * is low cost: the result set loader will have provided a "dummy" column
 * writer that simply discards the value. This is just as fast as having the
 * reader use if-statements or a table to determine which columns to save.
 * <p>
 * A reader may be "late schema", true "schema on read." In this case, the
 * reader simply tells the result set loader to create a new column reader
 * on the fly. The framework will work out if that new column is to be
 * projected and will return either a real column writer (projected column)
 * or a dummy column writer (unprojected column.)</li>
 * <li>The reader then reads batches of data until all data is read. The
 * result set loader signals when a batch is full; the reader should not
 * worry about this detail itself.</li>
 * <li>The reader then releases its resources.</li>
 * </ul>
 */

public abstract class AbstractScanFramework<T extends SchemaNegotiator> implements ScanOperatorEvents {

  // Inputs

  protected final List<SchemaPath> projection;
  protected MajorType nullType;
  protected int maxBatchRowCount;
  protected int maxBatchByteCount;
  protected OperatorContext context;

  // Internal state

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
