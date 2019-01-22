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
package org.apache.drill.exec.physical.impl.scan;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.record.VectorContainer;

/**
 * Manages a row batch reader through its lifecycle. Created when the reader
 * is opened, discarded when the reader is closed. Encapsulates state that
 * follows the life of the reader. This moves common scanner state out of
 * each reader in order to make the reader as simple as possible.
 * <p>
 * This class is private to the scan operator and is not meant to be used,
 * or even visible, outside of that operator itself. Instead, all reader-specific
 * functionality should be in the {@link RowBatchReader} subclass.
 */

class ReaderState {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReaderState.class);

  private enum State { START, LOOK_AHEAD, LOOK_AHEAD_WITH_EOF, ACTIVE, EOF, CLOSED };

  final ScanOperatorExec scanOp;
  private final RowBatchReader reader;
  private State state = State.START;
  private VectorContainer lookahead;
  private int schemaVersion = -1;

  public ReaderState(ScanOperatorExec scanOp, RowBatchReader reader) {
    this.scanOp = scanOp;
    this.reader = reader;
  }

  /**
   * Open the next available reader, if any, preparing both the
   * reader and table loader.
   * @return true if another reader is active, false if no more
   * readers are available
   */

  boolean open() {

    // Open the reader. This can fail. if it does, clean up.

    try {

      // The reader can return a "soft" failure: the open worked, but
      // the file is empty, non-existent or some other form of "no data."
      // Handle this by immediately moving to EOF. The scanner will quietly
      // pass over this reader and move onto the next, if any.

      if (! reader.open()) {
        state = State.EOF;
        return false;
      }

    // When catching errors, leave the reader member set;
    // we must close it on close() later.

    } catch (UserException e) {

      // Throw user exceptions as-is

      throw e;
    } catch (Throwable t) {

      // Wrap all others in a user exception.

      throw UserException.executionError(t)
        .addContext("Open failed for reader", reader.name())
        .build(logger);
    }

    state = State.ACTIVE;
    return true;
  }

  /**
   * Prepare the schema for this reader. Called for the first reader within a
   * scan batch, if the reader returns <tt>true</tt> from <tt>open()</tt>. If
   * this is an early-schema reader, then the result set loader already has
   * the proper value vectors set up. If this is a late-schema reader, we must
   * read one batch to get the schema, then set aside the data for the next
   * call to <tt>next()</tt>.
   * <p>
   * Semantics for all readers:
   * <ul>
   * <li>If the file was not found, <tt>open()</tt> returned false and this
   * method should never be called.</li>
   * </ul>
   * <p>
   * Semantics for early-schema readers:
   * <ul>
   * <li>If if turned out that the file was
   * empty when trying to read the schema, <tt>open()</tt> returned false
   * and this method should never be called.</tt>
   * <li>Otherwise, if a schema was available, then the schema is already
   * set up in the result set loader as the result of schema negotiation, and
   * this method simply returns <tt>true</tt>.
   * </ul>
   * <p>
   * Semantics for late-schema readers:
   * <ul>
   * <li>This method will ask the reader to
   * read a batch. If the reader hits EOF before finding any data, this method
   * will return false, indicating that no schema is available.</li>
   * <li>If the reader can read enough of the file to
   * figure out the schema, but the file has no data, then this method will
   * return <tt>true</tt> and a schema will be available. The first call to
   * <tt>next()</tt> will report EOF.</li>
   * <li>Otherwise, this method returns true, sets up an empty batch with the
   * schema, saves the data batch, and will return that look-ahead batch on the
   * first call to <tt>next()</tt>.</li>
   * </ul>
   * @return true if the schema was read, false if EOF was reached while trying
   * to read the schema.
   */
  protected boolean buildSchema() {

    VectorContainer container = reader.output();

    if (container != null) {

      // Bind the output container to the output of the scan operator.
      // This returns an empty batch with the schema filled in.

      scanOp.containerAccessor.setContainer(container);
      schemaVersion = reader.schemaVersion();
      return true;
    }

    // Late schema. Read a batch.

    if (! next()) {
      return false;
    }
    container = reader.output();
    if (container.getRecordCount() == 0) {
      return true;
    }

    // The reader returned actual data. Just forward the schema
    // in a dummy container, saving the data for next time.

    assert lookahead == null;
    lookahead = new VectorContainer(scanOp.context.getAllocator(), scanOp.containerAccessor.getSchema());
    lookahead.setRecordCount(0);
    lookahead.exchange(scanOp.containerAccessor.getOutgoingContainer());
    state = state == State.EOF? State.LOOK_AHEAD_WITH_EOF : State.LOOK_AHEAD;
    return true;
  }

  protected boolean next() {
    switch (state) {
    case LOOK_AHEAD:
    case LOOK_AHEAD_WITH_EOF:
      // Use batch previously read.
      assert lookahead != null;
      lookahead.exchange(scanOp.containerAccessor.getOutgoingContainer());
      assert lookahead.getRecordCount() == 0;
      lookahead = null;
      state = state == State.LOOK_AHEAD_WITH_EOF ? State.EOF : State.ACTIVE;
      return true;

    case ACTIVE:
      return readBatch();

    case EOF:
      return false;

    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

  /**
   * Read a batch from the current reader.
   * <p>
   * Expected semantics for the reader's <tt>next()</tt> method:
   * <ul>
   * <li>Non-empty batch and return true: data returned and more
   * data is (probably) available.</li>
   * <li>Empty batch and return true: data returned but it is the last
   * batch; EOF was reached while reading the batch.</li>
   * <li>Empty batch and return false: EOF reached, discard the
   * empty batch. (An inefficient way to indicate EOF since a set
   * of vectors is allocated, then discarded. The previous result
   * is preferred when possible.</li>
   * <li>Empty batch and return true: An odd case that is allowed;
   * the batch is discarded and <tt>next()</tt> is called again.</li>
   * </ul>
   * In short:
   * <ul>
   * <li>A non-empty batch says that there is data to return.</li>
   * <li>The return code says whether <tt>next()</tt> should be called
   * again.</li>
   * </ul>
   *
   * @return true if a batch was read, false if the reader hit EOF
   */

  private boolean readBatch() {

    // Try to read a batch. This may fail. If so, clean up the
    // mess.

    boolean more;
    try {
      more = reader.next();
      if (! more) {
        state = State.EOF;
      }
    } catch (UserException e) {
      throw e;
    } catch (Throwable t) {
      throw UserException.executionError(t)
        .addContext("Read failed for reader", reader.name())
        .build(logger);
    }

    VectorContainer output = reader.output();
    if (! more && output.getRecordCount() == 0) {
      return false;
    }

    // Late schema readers may change their schema between batches.
    // Early schema changes only on the first batch of the next
    // reader. (This is not a hard and fast rule, only a definition:
    // a reader that starts with a schema, but later changes it, has
    // morphed from an early- to late-schema reader.)

    int newVersion = reader.schemaVersion();
    if (newVersion > schemaVersion) {
      scanOp.containerAccessor.setContainer(output);
      schemaVersion = newVersion;
    }
    return true;
  }

  /**
   * Close the current reader. The hard part is handling the possible
   * error conditions, and cleaning up despite those errors.
   */

  void close() {
    if (state == State.CLOSED) {
      return; // TODO: Test this path
    }

    // Close the reader. This can fail.

    try {
      reader.close();
    } catch (UserException e) {
      throw e;
    } catch (Throwable t) {
      throw UserException.executionError(t)
        .addContext("Close failed for reader", reader.name())
        .build(logger);
    } finally {

      // Will not throw exceptions

      if (lookahead != null) {
        lookahead.clear(); // TODO: Test this path
        lookahead = null;
      }
      state = State.CLOSED;
    }
  }
}
