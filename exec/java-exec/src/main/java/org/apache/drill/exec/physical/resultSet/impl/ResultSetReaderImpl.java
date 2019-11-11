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

import org.apache.drill.exec.physical.impl.protocol.BatchAccessor;
import org.apache.drill.exec.physical.resultSet.ResultSetReader;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.physical.rowSet.RowSets;
import org.apache.drill.shaded.guava.com.google.common.annotations.VisibleForTesting;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

public class ResultSetReaderImpl implements ResultSetReader {

  @VisibleForTesting
  protected enum State {
      START,
      BATCH,
      DETACHED,
      CLOSED
  }

  private final boolean autoRelease;
  private State state = State.START;
  private int priorSchemaVersion;
  private BatchAccessor batch;
  private RowSetReader rowSetReader;

  public ResultSetReaderImpl(boolean autoRelease) {
    this.autoRelease = autoRelease;
  }

  public ResultSetReaderImpl() {
    this(false);
  }

  @Override
  public void start(BatchAccessor batch) {
    autoRelease();
    Preconditions.checkState(state != State.CLOSED, "Reader is closed");
    Preconditions.checkState(state != State.BATCH,
        "Call detach/release before starting another batch");
    Preconditions.checkState(state == State.START ||
        priorSchemaVersion <= batch.schemaVersion());
    boolean newSchema = state == State.START ||
        priorSchemaVersion != batch.schemaVersion();
    this.batch = batch;
    state = State.BATCH;

    // If new schema, discard the old reader (if any, and create
    // a new one that matches the new schema. If not a new schema,
    // then the old reader is reused: it points to vectors which
    // Drill requires be the same vectors as the previous batch,
    // but with different buffers.

    if (newSchema) {
      rowSetReader = RowSets.wrap(batch).reader();
      priorSchemaVersion = batch.schemaVersion();
    } else {
      rowSetReader.newBatch();
    }
  }

  @Override
  public RowSetReader reader() {
    Preconditions.checkState(state == State.BATCH, "Call start() before requesting the reader.");
    return rowSetReader;
  }

  @Override
  public void detach() {
    Preconditions.checkState(state == State.BATCH || state == State.DETACHED);
    state = State.DETACHED;
  }

  @Override
  public void release() {
    if (state != State.DETACHED) {
      detach();
      batch.release();
    }
  }

  @Override
  public void close() {
    autoRelease();
    state = State.CLOSED;
  }

  private void autoRelease() {
    if (autoRelease && state == State.BATCH) {
      release();
    }
  }

  @VisibleForTesting
  protected State state() { return state; }
}
