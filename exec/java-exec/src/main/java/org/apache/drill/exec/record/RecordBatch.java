/**
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
package org.apache.drill.exec.record;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

/**
 * A record batch contains a set of field values for a particular range of records. In the case of a record batch
 * composed of ValueVectors, ideally a batch fits within L2 cache (~256k per core). The set of value vectors do not
 * change unless the next() IterOutcome is a *_NEW_SCHEMA type.
 *
 * A key thing to know is that the Iterator provided by record batch must align with the rank positions of the field ids
 * provided utilizing getValueVectorId();
 */
public interface RecordBatch extends VectorAccessible, AutoCloseable {

  /* max batch size, limited by 2-byte-length in SV2 : 65536 = 2^16 */
  public static final int MAX_BATCH_SIZE = 65536;

  /**
   * Describes the outcome of a RecordBatch being incremented forward.
   */
  public static enum IterOutcome {
    NONE, // No more records were found.
    OK, // A new range of records have been provided.
    OK_NEW_SCHEMA, // A full collection of records
    STOP, // Informs parent nodes that the query has terminated. In this case, a consumer can consume their QueryContext
          // to understand the current state of things.
    NOT_YET, // used by batches that haven't received incoming data yet.
    OUT_OF_MEMORY // an upstream operator was unable to allocate memory. A batch receiving this should release memory if it can
  }

  public static enum SetupOutcome {
    OK, OK_NEW_SCHEMA, FAILED
  }

  /**
   * Access the FragmentContext of the current query fragment. Useful for reporting failure information or other query
   * level information.
   *
   * @return
   */
  FragmentContext getContext();

  /**
   * Inform child nodes that this query should be terminated. Child nodes should utilize the QueryContext to determine
   * what has happened.
   */
  void kill(boolean sendUpstream);

  SelectionVector2 getSelectionVector2();

  SelectionVector4 getSelectionVector4();

  VectorContainer getOutgoingContainer();

  /**
   * Update the data in each Field reading interface for the next range of records. Once a RecordBatch returns an
   * IterOutcome.NONE, the consumer should no longer next(). Behavior at this point is undetermined and likely to throw
   * an exception.
   *
   * @return An IterOutcome describing the result of the iteration.
   */
  IterOutcome next();

  /**
   * Get a writable version of this batch. Takes over owernship of existing buffers.
   *
   * @return
   */
  WritableBatch getWritableBatch();
}
