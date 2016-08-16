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

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;

/**
 * A record batch contains a set of field values for a particular range of
 * records.
 * <p>
 *   In the case of a record batch composed of ValueVectors, ideally a batch
 *   fits within L2 cache (~256kB per core).  The set of value vectors does
 *   not change except during a call to {@link #next()} that returns
 *   {@link IterOutcome#OK_NEW_SCHEMA} value.
 * </p>
 * <p>
 *   A key thing to know is that the Iterator provided by a record batch must
 *   align with the rank positions of the field IDs provided using
 *   {@link getValueVectorId}.
 * </p>
 */
public interface RecordBatch extends VectorAccessible {

  /** max batch size, limited by 2-byte length in SV2: 65536 = 2^16 */
  public static final int MAX_BATCH_SIZE = 65536;

  /**
   * Describes the outcome of incrementing RecordBatch forward by a call to
   * {@link #next()}.
   * <p>
   *   Key characteristics of the return value sequence:
   * </p>
   * <ul>
   *   <li>
   *     {@code OK_NEW_SCHEMA} always appears unless {@code STOP} appears.  (A
   *     batch returns {@code OK_NEW_SCHEMA} before returning {@code NONE} even
   *     if the batch has zero rows.)
   *   </li>
   *   <li>{@code OK_NEW_SCHEMA} always appears before {@code OK} appears.</li>
   *   <li>
   *     The last value is always {@code NONE} or {@code STOP}, and {@code NONE}
   *     and {@code STOP} appear only as the last value.
   *   </li>
   * </ul>
   * <p>
   *  <strong>Details</strong>:
   * </p>
   * <p>
   *   For normal completion, the basic sequence of return values from calls to
   *   {@code next()} on a {@code RecordBatch} is:
   * </p>
   * <ol>
   *   <li>
   *     an {@link #OK_NEW_SCHEMA} value followed by zero or more {@link #OK}
   *     values,
   *   </li>
   *   <li>
   *     zero or more subsequences each having an {@code OK_NEW_SCHEMA} value
   *     followed by zero or more {@code OK} values, and then
   *   </li>
   *   <li>
   *     a {@link #NONE} value.
   *   </li>
   * </ol>
   * <p>
   *   In addition to that basic sequence, {@link #NOT_YET} and
   *   {@link #OUT_OF_MEMORY} values can appear anywhere in the subsequence
   *   before the terminal value ({@code NONE} or {@code STOP}).
   * </p>
   * <p>
   *   For abnormal termination, the sequence is truncated (before the
   *   {@code NONE}) and ends with {@link #STOP}.  That is, the sequence begins
   *   with a subsequence that is some prefix of a normal-completion sequence
   *   and that does not contain {@code NONE}, and ends with {@code STOP}.
   * </p>
   * <p>
   *   (The normal-completion return sequence is matched by the following
   *   regular-expression-style grammar:
   *   <pre>
   *     ( ( NOT_YET | OUT_OF_MEMORY )*  OK_NEW_SCHEMA
   *       ( NOT_YET | OUT_OF_MEMORY )*  OK )*
   *     )+
   *     ( NOT_YET | OUT_OF_+MEMORY )*  NONE
   *   </pre>
   *   )
   * </p>
   */
  public static enum IterOutcome {
    /**
     * Normal completion of batch.
     * <p>
     *   The call to {@link #next()}
     *   read no records,
     *   the batch has and will have no more results to return,
     *   and {@code next()} must not be called again.
     * </p>
     * <p>
     *   This value will be returned only after {@link #OK_NEW_SCHEMA} has been
     *   returned at least once (not necessarily <em>immediately</em> after).
     * </p>
     */
    NONE,

    /**
     * Zero or more records with same schema.
     * <p>
     *   The call to {@link #next()}
     *   read zero or more records,
     *   the schema has not changed since the last time {@code OK_NEW_SCHEMA}
     *     was returned,
     *   and the batch will have more results to return (at least completion or
     *     abnormal termination ({@code NONE} or {@code STOP})).
     *     ({@code next()} should be called again.)
     * </p>
     * <p>
     *   This will be returned only after {@link #OK_NEW_SCHEMA} has been
     *   returned at least once (not necessarily <em>immediately</em> after).
     * </p>
     */
    OK,

    /**
     * New schema, maybe with records.
     * <p>
     *   The call to {@link #next()}
     *   changed the schema and vector structures
     *   and read zero or more records,
     *   and the batch will have more results to return (at least completion or
     *     abnormal termination ({@code NONE} or {@code STOP})).
     *     ({@code next()} should be called again.)
     * </p>
     */
    OK_NEW_SCHEMA,

    /**
     * Non-completion (abnormal) termination.
     * <p>
     *   The call to {@link #next()}
     *   reports that the query has terminated other than by normal completion,
     *   and that the caller must not call any of the schema-access or
     *   data-access methods nor call {@code next()} again.
     * </p>
     * <p>
     *   The caller can consume its QueryContext to understand the current state
     *   of things.
     * </p>
     */
    STOP,

    /**
     * No data yet.
     * <p>
     *   The call to {@link #next()}
     *   read no data,
     *   and the batch will have more results to return in the future (at least
     *     completion or abnormal termination ({@code NONE} or {@code STOP})).
     *   The caller should call {@code next()} again, but should do so later
     *     (including by returning {@code NOT_YET} to its caller).
     * </p>
     * <p>
     *   Normally, the caller should perform any locally available work while
     *   waiting for incoming data from the callee, for example, doing partial
     *   sorts on already received data while waiting for additional data to
     *   sort.
     * </p>
     * <p>
     *   Used by batches that haven't received incoming data yet.
     * </p>
     */
    NOT_YET,

    /**
     * Out of memory (not fatal).
     * <p>
     *   The call to {@link #next()},
     *   including upstream operators, was unable to allocate memory
     *   and did not read any records,
     *   and the batch will have more results to return (at least completion or
     *     abnormal termination ({@code NONE} or {@code STOP})).
     *   The caller should release memory if it can (including by returning
     *     {@code OUT_OF_MEMORY} to its caller) and call {@code next()} again.
     * </p>
     */
    OUT_OF_MEMORY
  }

  /**
   * Gets the FragmentContext of the current query fragment.  Useful for
   * reporting failure information or other query-level information.
   */
  public FragmentContext getContext();

  /**
   * Gets the current schema of this record batch.
   * <p>
   *   May be called only when the most recent call to {@link #next}, if any,
   *   returned {@link #OK_NEW_SCHEMA} or {@link #OK}.
   * </p>
   * <p>
   *   The schema changes when and only when {@link #next} returns
   *   {@link #OK_NEW_SCHEMA}.
   * </p>
   */
  @Override
  public BatchSchema getSchema();

  /**
   * Gets the number of records that are within this record.
   */
  @Override
  public int getRecordCount();

  /**
   * Informs child nodes that this query should be terminated.  Child nodes
   * should use the QueryContext to determine what has happened.
   */
  public void kill(boolean sendUpstream);

  public VectorContainer getOutgoingContainer();

  /**
   * Gets the value vector type and ID for the given schema path.  The
   * TypedFieldId should store a fieldId which is the same as the ordinal
   * position of the field within the Iterator provided this class's
   * implementation of Iterable<ValueVector>.
   *
   * @param path
   *          The path where the vector should be located.
   * @return The local field id associated with this vector. If no field matches this path, this will return a null
   *         TypedFieldId
   */
  @Override
  public abstract TypedFieldId getValueVectorId(SchemaPath path);

  @Override
  public abstract VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids);

  /**
   * Updates the data in each Field reading interface for the next range of
   * records.
   * <p>
   *   Once a RecordBatch's {@code next()} has returned {@link IterOutcome#NONE}
   *   or {@link IterOutcome#STOP}, the consumer should no longer call
   *   {@code next()}.  Behavior at this point is undefined and likely to
   *   throw an exception.
   * </p>
   * <p>
   *   See {@link IterOutcome} for the protocol (possible sequences of return
   *   values).
   * </p>
   *
   *
   * @return An IterOutcome describing the result of the iteration.
   */
  public IterOutcome next();

  /**
   * Gets a writable version of this batch.  Takes over ownership of existing
   * buffers.
   */
  public WritableBatch getWritableBatch();

}
