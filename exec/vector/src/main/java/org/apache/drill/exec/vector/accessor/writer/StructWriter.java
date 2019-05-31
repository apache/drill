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

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.ProjectionType;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.writer.AbstractArrayWriter.ArrayObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.dummy.DummyArrayWriter;
import org.apache.drill.exec.vector.complex.AbstractStructVector;
import org.apache.drill.exec.vector.complex.StructVector;
import org.apache.drill.exec.vector.complex.RepeatedStructVector;

/**
 * Writer for a Drill Map type. Maps are actually tuples, just like rows.
 */

public abstract class StructWriter extends AbstractTupleWriter {

  /**
   * Wrap the outer index to avoid incrementing the array index
   * on the call to <tt>nextElement().</tt> For maps, the increment
   * is done at the struct level, not the column level.
   */

  private static class MemberWriterIndex implements ColumnWriterIndex {
    private ColumnWriterIndex baseIndex;

    private MemberWriterIndex(ColumnWriterIndex baseIndex) {
      this.baseIndex = baseIndex;
    }

    @Override public int rowStartIndex() { return baseIndex.rowStartIndex(); }
    @Override public int vectorIndex() { return baseIndex.vectorIndex(); }
    @Override public void nextElement() { }
    @Override public void rollover() { }

    @Override public ColumnWriterIndex outerIndex() {
      return baseIndex.outerIndex();
    }

    @Override
    public String toString() {
      return new StringBuilder()
        .append("[")
        .append(getClass().getSimpleName())
        .append(" baseIndex = ")
        .append(baseIndex.toString())
        .append("]")
        .toString();
    }
  }

  /**
   * Writer for a single (non-array) struct. Clients don't really "write" maps;
   * rather, this writer is a holder for the columns within the struct, and those
   * columns are what is written.
   */

  protected static class SingleStructWriter extends StructWriter {
    private final StructVector structVector;

    protected SingleStructWriter(ColumnMetadata schema, StructVector vector, List<AbstractObjectWriter> writers) {
      super(schema, writers);
      structVector = vector;
    }

    @Override
    public void endWrite() {
      super.endWrite();

      // A non repeated struct has a field that holds the value count.
      // Update it. (A repeated struct uses the offset vector's value count.)
      // Special form of set value count: used only for
      // this class to avoid setting the value count of children.
      // Setting these counts was already done. Doing it again
      // will corrupt nullable vectors because the writers don't
      // set the "lastSet" field of nullable vector accessors,
      // and the initial value of -1 will cause all values to
      // be overwritten.

      structVector.setMapValueCount(vectorIndex.vectorIndex());
    }

    @Override
    public void preRollover() {
      super.preRollover();
      structVector.setMapValueCount(vectorIndex.rowStartIndex());
    }
  }

  /**
   * Writer for a an array of maps. A single array index coordinates writes
   * to the constituent member vectors so that, say, the values for (row 10,
   * element 5) all occur to the same position in the columns within the struct.
   * Since the struct is an array, it has an associated offset vector, which the
   * parent array writer is responsible for maintaining.
   */

  protected static class ArrayStructWriter extends StructWriter {

    protected ArrayStructWriter(ColumnMetadata schema, List<AbstractObjectWriter> writers) {
      super(schema, writers);
    }

    @Override
    public void bindIndex(ColumnWriterIndex index) {

      // This is a repeated struct, so the provided index is an array element
      // index. Convert this to an index that will not increment the element
      // index on each write so that a struct with three members, say, won't
      // increment the index for each member. Rather, the index must be
      // incremented at the array level.

      bindIndex(index, new MemberWriterIndex(index));
    }
  }

  protected static class DummyStructWriter extends StructWriter {

    protected DummyStructWriter(ColumnMetadata schema,
                                List<AbstractObjectWriter> writers) {
      super(schema, writers);
    }

    @Override
    public ProjectionType projectionType(String columnName) { return ProjectionType.UNPROJECTED; }
  }

  protected static class DummyArrayStructWriter extends StructWriter {

    protected DummyArrayStructWriter(ColumnMetadata schema,
                                     List<AbstractObjectWriter> writers) {
      super(schema, writers);
    }

    @Override
    public ProjectionType projectionType(String columnName) { return ProjectionType.UNPROJECTED; }
  }

  protected final ColumnMetadata mapColumnSchema;

  protected StructWriter(ColumnMetadata schema, List<AbstractObjectWriter> writers) {
    super(schema.mapSchema(), writers);
    mapColumnSchema = schema;
  }

  public static TupleObjectWriter buildMap(ColumnMetadata schema, StructVector vector,
      List<AbstractObjectWriter> writers) {
    StructWriter structWriter;
    if (schema.isProjected()) {

      // Vector is not required for a struct writer; the struct's columns
      // are written, but not the (non-array) struct.

      structWriter = new SingleStructWriter(schema, vector, writers);
    } else {
      assert vector == null;
      structWriter = new DummyStructWriter(schema, writers);
    }
    return new TupleObjectWriter(structWriter);
  }

  public static ArrayObjectWriter buildMapArray(ColumnMetadata schema,
      RepeatedStructVector repeatedStructVector,
      List<AbstractObjectWriter> writers) {
    StructWriter structWriter;
    if (schema.isProjected()) {
      assert repeatedStructVector != null;
      structWriter = new ArrayStructWriter(schema, writers);
    } else {
      assert repeatedStructVector == null;
      structWriter = new DummyArrayStructWriter(schema, writers);
    }
    TupleObjectWriter mapArray = new TupleObjectWriter(structWriter);
    AbstractArrayWriter arrayWriter;
    if (schema.isProjected()) {
      arrayWriter = new ObjectArrayWriter(schema,
          repeatedStructVector.getOffsetVector(),
          mapArray);
    } else  {
      arrayWriter = new DummyArrayWriter(schema, mapArray);
    }
    return new ArrayObjectWriter(arrayWriter);
  }

  public static AbstractObjectWriter buildStructWriter(ColumnMetadata schema,
                                                       AbstractStructVector vector,
                                                       List<AbstractObjectWriter> writers) {
    if (schema.isArray()) {
      return StructWriter.buildMapArray(schema,
          (RepeatedStructVector) vector, writers);
    } else {
      return StructWriter.buildMap(schema, (StructVector) vector, writers);
    }
  }

  public static AbstractObjectWriter buildStructWriter(ColumnMetadata schema, AbstractStructVector vector) {
    assert schema.mapSchema().size() == 0;
    return buildStructWriter(schema, vector, new ArrayList<AbstractObjectWriter>());
  }

  @Override
  public ColumnMetadata schema() { return mapColumnSchema; }
}
