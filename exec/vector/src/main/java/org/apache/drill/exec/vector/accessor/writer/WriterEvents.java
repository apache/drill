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
package org.apache.drill.exec.vector.accessor.writer;

import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;

/**
 * Internal interface used to control the behavior
 * of writers. Consumers of writers never use this method; it is
 * instead used by the code that implements writers.
 * <p>
 * Most methods here represents events in a state machine. The top-level
 * writer provides a set of public methods which trigger one or more of
 * these internal events. The events draw some fine distinctions between
 * top-level values and those nested within arrays. See each kind of
 * writer for the details.
 * <p>
 * The events also ensure symmetry between top-level and nested tuples,
 * especially those nested within an array. That is, an event cannot change
 * meaning depending on whether the tuple is top-level or nested within an
 * array. Instead, the order of calls, or selectively making or not making
 * calls, can change.
 */

public interface WriterEvents {

  /**
   * Bind the writer to a writer index.
   *
   * @param index the writer index (top level or nested for
   * arrays)
   */

  void bindIndex(ColumnWriterIndex index);

  ColumnWriterIndex writerIndex();

  /**
   * Start a write (batch) operation. Performs any vector initialization
   * required at the start of a batch (especially for offset vectors.)
   */

  void startWrite();

  /**
   * Start a new row. To be called only when a row is not active. To
   * restart a row, call {@link #restartRow()} instead.
   */

  void startRow();

  /**
   * End a value. Similar to {@link saveRow()}, but the save of a value
   * is conditional on saving the row. This version is primarily of use
   * in tuples nested inside arrays: it saves each tuple within the array,
   * advancing to a new position in the array. The update of the array's
   * offset vector based on the cumulative value saves is done when
   * saving the row.
   */

  void endArrayValue();

  /**
   * During a writer to a row, rewind the the current index position to
   * restart the row.
   * Done when abandoning the current row, such as when filtering out
   * a row at read time.
   */

  void restartRow();

  /**
   * Saves a row. Commits offset vector locations and advances each to
   * the next position. Can be called only when a row is active.
   */

  void saveRow();

  /**
   * End a batch: finalize any vector values.
   */

  void endWrite();

  /**
   * The vectors backing this vector are about to roll over. Finish
   * the current batch up to, but not including, the current row.
   */

  void preRollover();

  /**
   * The vectors backing this writer rolled over. This means that data
   * for the current row has been rolled over into a new vector. Offsets
   * and indexes should be shifted based on the understanding that data
   * for the current row now resides at the start of a new vector instead
   * of its previous location elsewhere in an old vector.
   */

  void postRollover();

  /**
   * Return the last write position in the vector. This may be the
   * same as the writer index position (if the vector was written at
   * that point), or an earlier point. In either case, this value
   * points to the last valid value in the vector.
   *
   * @return index of the last valid value in the vector
   */

  int lastWriteIndex();
}
