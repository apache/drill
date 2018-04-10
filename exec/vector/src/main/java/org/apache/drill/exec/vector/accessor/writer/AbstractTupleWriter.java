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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;

/**
 * Implementation for a writer for a tuple (a row or a map.) Provides access to each
 * column using either a name or a numeric index.
 * <p>
 * A tuple maintains an internal state needed to handle dynamic column additions.
 * The state identifies the amount of "catch up" needed to get the new column into
 * the same state as the existing columns. The state is also handy for understanding
 * the tuple lifecycle. This lifecycle works for all three cases of:
 * <ul>
 * <li>Top-level tuple (row).</li>
 * <li>Nested tuple (map).</li>
 * <li>Array of tuples (repeated map).</li>
 * </ul>
 *
 * Specifically, the transitions, for batch, row and array events, are:
 *
 * <table border=1>
 * <tr><th>Public API</th><th>Tuple Event</th><th>State Transition</th>
 *     <th>Child Event</th></tr>
 * <tr><td>(Start state)</td>
 *     <td>&mdash;</td>
 *     <td>IDLE</td>
 *     <td>&mdash;</td></tr>
 * <tr><td>startBatch()</td>
 *     <td>startWrite()</td>
 *     <td>IDLE &rarr; IN_WRITE</td>
 *     <td>startWrite()</td></tr>
 * <tr><td>start() (new row)</td>
 *     <td>startRow()</td>
 *     <td>IN_WRITE &rarr; IN_ROW</td>
 *     <td>startRow()</td></tr>
 * <tr><td>start() (without save)</td>
 *     <td>restartRow()</td>
 *     <td>IN_ROW &rarr; IN_ROW</td>
 *     <td>restartRow()</td></tr>
 * <tr><td>save() (array)</td>
 *     <td>saveValue()</td>
 *     <td>IN_ROW &rarr; IN_ROW</td>
 *     <td>saveValue()</td></tr>
 * <tr><td rowspan=2>save() (row)</td>
 *     <td>saveValue()</td>
 *     <td>IN_ROW &rarr; IN_ROW</td>
 *     <td>saveValue()</td></tr>
 * <tr><td>saveRow()</td>
 *     <td>IN_ROW &rarr; IN_WRITE</td>
 *     <td>saveRow()</td></tr>
 * <tr><td rowspan=2>end batch</td>
 *     <td>&mdash;</td>
 *     <td>IN_ROW &rarr; IDLE</td>
 *     <td>endWrite()</td></tr>
 * <tr><td>&mdash;</td>
 *     <td>IN_WRITE &rarr; IDLE</td>
 *     <td>endWrite()</td></tr>
 * </table>
 *
 * Notes:
 * <ul>
 * <li>For the top-level tuple, a special case occurs with ending a batch. (The
 *     method for doing so differs depending on implementation.) If a row is active,
 *     then that row's values are discarded. Then, the batch is ended.</li>
 * </ul>
 */

public abstract class AbstractTupleWriter implements TupleWriter, WriterEvents {

  /**
   * Generic object wrapper for the tuple writer.
   */

  public static class TupleObjectWriter extends AbstractObjectWriter {

    private AbstractTupleWriter tupleWriter;

    public TupleObjectWriter(ColumnMetadata schema, AbstractTupleWriter tupleWriter) {
      super(schema);
      this.tupleWriter = tupleWriter;
    }

    @Override
    public ObjectType type() { return ObjectType.TUPLE; }

    @Override
    public void set(Object value) { tupleWriter.setObject(value); }

    @Override
    public TupleWriter tuple() { return tupleWriter; }

    @Override
    public WriterEvents events() { return tupleWriter; }

    @Override
    public void bindListener(TupleWriterListener listener) {
      tupleWriter.bindListener(listener);
    }

    @Override
    public void dump(HierarchicalFormatter format) {
      format
        .startObject(this)
        .attribute("tupleWriter");
      tupleWriter.dump(format);
      format.endObject();
    }
  }

  /**
   * Tracks the write state of the tuple to allow applying the correct
   * operations to newly-added columns to synchronize them with the rest
   * of the tuple.
   */

  public enum State {
    /**
     * No write is in progress. Nothing need be done to newly-added
     * writers.
     */
    IDLE,

    /**
     * <tt>startWrite()</tt> has been called to start a write operation
     * (start a batch), but <tt>startValue()</tt> has not yet been called
     * to start a row (or value within an array). <tt>startWrite()</tt> must
     * be called on newly added columns.
     */

    IN_WRITE,

    /**
     * Both <tt>startWrite()</tt> and <tt>startValue()</tt> has been called on
     * the tuple to prepare for writing values, and both must be called on
     * newly-added vectors.
     */

    IN_ROW
  }

  protected final TupleMetadata schema;
  protected final List<AbstractObjectWriter> writers;
  protected ColumnWriterIndex vectorIndex;
  protected ColumnWriterIndex childIndex;
  protected TupleWriterListener listener;
  protected State state = State.IDLE;

  protected AbstractTupleWriter(TupleMetadata schema, List<AbstractObjectWriter> writers) {
    this.schema = schema;
    this.writers = writers;
  }

  protected AbstractTupleWriter(TupleMetadata schema) {
    this(schema, new ArrayList<AbstractObjectWriter>());
  }

  protected void bindIndex(ColumnWriterIndex index, ColumnWriterIndex childIndex) {
    vectorIndex = index;
    this.childIndex = childIndex;

    for (int i = 0; i < writers.size(); i++) {
      writers.get(i).events().bindIndex(childIndex);
    }
  }

  @Override
  public void bindIndex(ColumnWriterIndex index) {
    bindIndex(index, index);
  }

  @Override
  public ColumnWriterIndex writerIndex() { return vectorIndex; }

  /**
   * Add a column writer to an existing tuple writer. Used for implementations
   * that support "live" schema evolution: column discovery while writing.
   * The corresponding metadata must already have been added to the schema.
   *
   * @param colWriter the column writer to add
   */

  public int addColumnWriter(AbstractObjectWriter colWriter) {
    assert writers.size() == schema.size();
    int colIndex = schema.addColumn(colWriter.schema());
    writers.add(colWriter);
    colWriter.events().bindIndex(childIndex);
    if (state != State.IDLE) {
      colWriter.events().startWrite();
      if (state == State.IN_ROW) {
        colWriter.events().startRow();
      }
    }
    return colIndex;
  }

  @Override
  public int addColumn(ColumnMetadata column) {
    if (listener == null) {
      throw new UnsupportedOperationException("addColumn");
    }
    AbstractObjectWriter colWriter = (AbstractObjectWriter) listener.addColumn(this, column);
    return addColumnWriter(colWriter);
  }

  @Override
  public int addColumn(MaterializedField field) {
    if (listener == null) {
      throw new UnsupportedOperationException("addColumn");
    }
    AbstractObjectWriter colWriter = (AbstractObjectWriter) listener.addColumn(this, field);
    return addColumnWriter(colWriter);
  }

  @Override
  public TupleMetadata schema() { return schema; }

  @Override
  public int size() { return schema().size(); }

  @Override
  public void startWrite() {
    assert state == State.IDLE;
    state = State.IN_WRITE;
    for (int i = 0; i < writers.size();  i++) {
      writers.get(i).events().startWrite();
    }
  }

  @Override
  public void startRow() {
    // Must be in a write. Can start a row only once.
    // To restart, call restartRow() instead.

    assert state == State.IN_WRITE;
    state = State.IN_ROW;
    for (int i = 0; i < writers.size();  i++) {
      writers.get(i).events().startRow();
    }
  }

  @Override
  public void endArrayValue() {
    assert state == State.IN_ROW;
    for (int i = 0; i < writers.size();  i++) {
      writers.get(i).events().endArrayValue();
    }
  }

  @Override
  public void restartRow() {

    // Rewind is normally called only when a value is active: it resets
    // pointers to allow rewriting the value. However, if this tuple
    // is nested in an array, then the array entry could have been
    // saved (state here is IN_WRITE), but the row as a whole has
    // not been saved. Thus, we must also allow a rewind() while in
    // the IN_WRITE state to set the pointers back to the start of
    // the current row.

    assert state == State.IN_ROW;
    for (int i = 0; i < writers.size();  i++) {
      writers.get(i).events().restartRow();
    }
  }

  @Override
  public void saveRow() {
    assert state == State.IN_ROW;
    for (int i = 0; i < writers.size();  i++) {
      writers.get(i).events().saveRow();
    }
    state = State.IN_WRITE;
  }

  @Override
  public void preRollover() {

    // Rollover can only happen while a row is in progress.

    assert state == State.IN_ROW;
    for (int i = 0; i < writers.size();  i++) {
      writers.get(i).events().preRollover();
    }
  }

  @Override
  public void postRollover() {

    // Rollover can only happen while a row is in progress.

    assert state == State.IN_ROW;
    for (int i = 0; i < writers.size();  i++) {
      writers.get(i).events().postRollover();
    }
  }

  @Override
  public void endWrite() {
    assert state != State.IDLE;
    for (int i = 0; i < writers.size();  i++) {
      writers.get(i).events().endWrite();
    }
    state = State.IDLE;
  }

  @Override
  public ObjectWriter column(int colIndex) {
    return writers.get(colIndex);
  }

  @Override
  public ObjectWriter column(String colName) {
    int index = schema.index(colName);
    if (index == -1) {
      throw new UndefinedColumnException(colName);
    }
    return writers.get(index);
  }

  @Override
  public void set(int colIndex, Object value) {
    ObjectWriter colWriter = column(colIndex);
    switch (colWriter.type()) {
    case ARRAY:
      colWriter.array().setObject(value);
      break;
    case SCALAR:
      colWriter.scalar().setObject(value);
      break;
    case TUPLE:
      colWriter.tuple().setObject(value);
      break;
    default:
      throw new IllegalStateException("Unexpected object type: " + colWriter.type());
    }
  }

  @Override
  public void setTuple(Object ...values) {
    setObject(values);
  }

  @Override
  public void setObject(Object value) {
    Object values[] = (Object[]) value;
    if (values.length != schema.size()) {
      throw new IllegalArgumentException(
          "Map has " + schema.size() +
          " columns, but value array has " +
          values.length + " values.");
    }
    for (int i = 0; i < values.length; i++) {
      set(i, values[i]);
    }
  }

  @Override
  public ScalarWriter scalar(int colIndex) {
    return column(colIndex).scalar();
  }

  @Override
  public ScalarWriter scalar(String colName) {
    return column(colName).scalar();
  }

  @Override
  public TupleWriter tuple(int colIndex) {
    return column(colIndex).tuple();
  }

  @Override
  public TupleWriter tuple(String colName) {
    return column(colName).tuple();
  }

  @Override
  public ArrayWriter array(int colIndex) {
    return column(colIndex).array();
  }

  @Override
  public ArrayWriter array(String colName) {
    return column(colName).array();
  }

  @Override
  public ObjectType type(int colIndex) {
    return column(colIndex).type();
  }

  @Override
  public ObjectType type(String colName) {
    return column(colName).type();
  }

  @Override
  public int lastWriteIndex() {
    return vectorIndex.vectorIndex();
  }

  @Override
  public void bindListener(TupleWriterListener listener) {
    this.listener = listener;
  }

  public void dump(HierarchicalFormatter format) {
    format
      .startObject(this)
      .attribute("vectorIndex", vectorIndex)
      .attribute("state", state)
      .attributeArray("writers");
    for (int i = 0; i < writers.size(); i++) {
      format.element(i);
      writers.get(i).dump(format);
    }
    format
      .endArray()
      .endObject();
  }
}
