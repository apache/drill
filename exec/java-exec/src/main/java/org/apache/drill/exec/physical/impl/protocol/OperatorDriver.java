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
package org.apache.drill.exec.physical.impl.protocol;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;

/**
 * State machine that drives the operator executable. Converts
 * between the iterator protocol and the operator executable protocol.
 * Implemented as a separate class in anticipation of eventually
 * changing the record batch (iterator) protocol.
 */

public class OperatorDriver {
  public enum State {

    /**
     * Before the first call to next().
     */

    START,

    /**
     * The first call to next() has been made and schema (only)
     * was returned. On the subsequent call to next(), return any
     * data that might have accompanied that first batch.
     */

    SCHEMA,

    /**
     * The second call to next() has been made and there is more
     * data to deliver on subsequent calls.
     */

    RUN,

    /**
     * No more data to deliver.
     */

    END,

    /**
     * An error occurred.
     */

    FAILED,

    /**
     * Operation was cancelled. No more batches will be returned,
     * but close() has not yet been called.
     */

    CANCELED,

    /**
     * close() called and resources are released. No more batches
     * will be returned, but close() has not yet been called.
     * (This state is semantically identical to FAILED, it exists just
     * in case an implementation needs to know the difference between the
     * END, FAILED and CANCELED states.)
     */

    CLOSED
  }

  private OperatorDriver.State state = State.START;

  /**
   * Operator context. The driver "owns" the context and is responsible
   * for closing it.
   */

  private final OperatorContext opContext;
  private final OperatorExec operatorExec;
  private final BatchAccessor batchAccessor;
  private int schemaVersion;

  public OperatorDriver(OperatorContext opContext, OperatorExec opExec) {
    this.opContext = opContext;
    this.operatorExec = opExec;
    batchAccessor = operatorExec.batchAccessor();
  }

  /**
   * Get the next batch. Performs initialization on the first call.
   * @return the iteration outcome to send downstream
   */

  public IterOutcome next() {
    try {
      switch (state) {
      case START:
        return start();
      case RUN:
        return doNext();
      default:
        OperatorRecordBatch.logger.debug("Extra call to next() in state " + state + ": " + operatorLabel());
        return IterOutcome.NONE;
      }
    } catch (UserException e) {
      cancelSilently();
      state = State.FAILED;
      throw e;
    } catch (Throwable t) {
      cancelSilently();
      state = State.FAILED;
      throw UserException.executionError(t)
        .addContext("Exception thrown from", operatorLabel())
        .build(OperatorRecordBatch.logger);
    }
  }

  /**
   * Cancels the operator before reaching EOF.
   */

  public void cancel() {
    try {
      switch (state) {
      case START:
      case RUN:
        cancelSilently();
        break;
      default:
        break;
      }
    } finally {
      state = State.CANCELED;
    }
  }

 /**
   * Start the operator executor. Bind it to the various contexts.
   * Then start the executor and fetch the first schema.
   * @return result of the first batch, which should contain
   * only a schema, or EOF
   */

  private IterOutcome start() {
    state = State.SCHEMA;
    if (operatorExec.buildSchema()) {
      schemaVersion = batchAccessor.schemaVersion();
      state = State.RUN;
      return IterOutcome.OK_NEW_SCHEMA;
    } else {
      state = State.END;
      return IterOutcome.NONE;
    }
  }

  /**
   * Fetch a record batch, detecting EOF and a new schema.
   * @return the <tt>IterOutcome</tt> for the above cases
   */

  private IterOutcome doNext() {
    if (! operatorExec.next()) {
      state = State.END;
      return IterOutcome.NONE;
    }
    int newVersion = batchAccessor.schemaVersion();
    if (newVersion != schemaVersion) {
      schemaVersion = newVersion;
      return IterOutcome.OK_NEW_SCHEMA;
    }
    return IterOutcome.OK;
  }

  /**
   * Implement a cancellation, and ignore any exception that is
   * thrown. We're already in trouble here, no need to keep track
   * of additional things that go wrong.
   */

  private void cancelSilently() {
    try {
      if (state == State.SCHEMA || state == State.RUN) {
        operatorExec.cancel();
      }
    } catch (Throwable t) {
      // Ignore; we're already in a bad state.
      OperatorRecordBatch.logger.error("Exception thrown from cancel() for " + operatorLabel(), t);
    }
  }

  private String operatorLabel() {
    return operatorExec.getClass().getCanonicalName();
  }

  public void close() {
    if (state == State.CLOSED) {
      return;
    }
    try {
      operatorExec.close();
    } catch (UserException e) {
      throw e;
    } catch (Throwable t) {
      throw UserException.executionError(t)
        .addContext("Exception thrown from", operatorLabel())
        .build(OperatorRecordBatch.logger);
    } finally {
      opContext.close();
      state = State.CLOSED;
    }
  }

  public BatchAccessor batchAccessor() {
    return batchAccessor;
  }

  public OperatorContext operatorContext() {
    return opContext;
  }
}
