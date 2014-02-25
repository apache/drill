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

package org.apache.drill.exec.store.ischema;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;

/**
 * Manages the value vectors used to implement columns in a record batch.
 * The vectors themselves are created by subclasses, so this class
 * handles the generic handling of the vectors.
 */
public abstract class EmptyVectorSet implements VectorSet {
  
  protected List<ValueVector> vectors;

  /**
   * Prepare to construct a new set of vectors.
   * The actual vectors will be created by subclasses
   * by the time our "next" procedure is invoked.
   */
  public EmptyVectorSet() {
    vectors = new ArrayList<ValueVector>();
  }

  /**
   * Prepare to read the next batch of rows.
   * @param maxRows
   */
  @Override
  public void beginBatch(int maxRows) {
    
    // Allocate memory for each column (value vector)
    for (ValueVector v: vectors) {
      AllocationHelper.allocate(v, maxRows, 100); // TODO: later, use configured size
    }
  }
  
  
  /**
   * Write a row to the value vectors. 
   * This is a routine to "assign generic objects to generic ValueVectors"
   * which can be overridden to optimize for fixed types of vectors and 
   * fixed types of values.
   * @param index - the position within the value vectors.
   * @param row - the objects to write into the vectors
   * @return true if there was room to write all the values.
   */
  @Override
  public boolean writeRowToVectors(int index, Object[] row) {
    for (int i=0; i<row.length; i++) {
      if (!setSafe(vectors.get(i), index, row[i])) {
        return false;
      }
    } 
    return true;
  }
  

  
  /**
   * Signal the end of the current batch.
   * @param actualRowCount
   */
  @Override
  public void endBatch(int actualRowCount) {
    
    // Finalize each of the value vectors.
    for (ValueVector v: vectors) {
      v.getMutator().setValueCount(actualRowCount);
    }
  }
  
  /**
   * When everything is done, free up the resources.
   */
  @Override
  public void cleanup() {
    for (ValueVector v: vectors) {
      v.close();
    }
  }
  
  
  /**
   * Make the value vectors visible to whomever needs them.
   */
  public List<ValueVector> getValueVectors() {
    return vectors;
  }
  

  /**
   * Estimate how many rows will fit in a given amount of memory.
   * Perfect estimates are nice, but things work out OK if
   * the estimates are a bit off.
   */
  @Override
  public int getEstimatedRowCount(int bufSize) {
    return Math.max(1, bufSize/getEstimatedRowSize());
  }


 
  /**
   * Estimate the size of an average row. Used for allocating memory.
   * Override when more information is known about the data.
   * @return bytes per row.
   */
  protected int getEstimatedRowSize() {
    
    // Add up the sizes of the vectors
    int size = 0;
    for (ValueVector v: vectors) {
      size += TypeHelper.getSize(v.getField().getType());  
    }
    return size;
  }
  
  
  /**
   * Helper function to create value vectors for a set of columns.
   * @param names - the names of the fields
   * @param types - the major types of the fields
   * @param allocator - a buffer allocator
   */
  protected void createVectors(String[] names, MajorType[] types, BufferAllocator allocator) {
    vectors = new ArrayList<ValueVector>(names.length);
    for (int i=0; i<names.length; i++) {
      vectors.add(createVector(names[i], types[i], allocator));
    }
  }
  
 
  /**
   * Create a value vector for a single column.
   * @param name - the name of the field
   * @param type - the type of the field
   * @param allocator - a buffer allocator
   * @return the new value vector.
   */
  private static ValueVector createVector(String name, MajorType type, BufferAllocator allocator) {
    return TypeHelper.getNewVector(field(name, type), allocator);
  }
  
  
  /**
   * Helper function to create a MaterializedField, used to create a ValueVector.
   * @param name - the name of the field
   * @param majorType - the type of the field
   * @return the MaterializedField
   */
  private static MaterializedField field(String name, MajorType majorType) {
    return MaterializedField.create(new SchemaPath(name, ExpressionPosition.UNKNOWN), majorType);
  }
  
  
  //////////////////////////////////////////////////////////////////
  //
  // The following section contains wrappers around ValueVectors.
  // The wrappers make it easier to create vectors and set values.
  //
  // A different approach is to enhance TypeHelper to provide
  // a uniform way to "setSafe" the common Java types into the type vectors.
  // (It does that already for some types, but Strings are a particular nuisance.)
  //
  // For now, only types used in information schema are implemented.
  //
  ///////////////////////////////////////////////////////////////////
  static final Charset UTF8 = Charset.forName("UTF-8");
  
  
  // Here are the types used in information schema. 
  public static final MajorType VARCHAR = Types.required(MinorType.VARCHAR);
  public static final MajorType INT = Types.required(MinorType.INT);
  //public static final MajorType NULLABLINT = Types.optional(MinorType.INT);
  
  
  /**
   * A generic routine to set a Java value into a value vector. It assumes the types are compatible.
   * When a subclass knows the types of its columns, it should use the strongly typed routines instead.
   * <P>
   * Note the value corresponds to what would be received by a varargs procedure.
   * Also note we are switching on minor type. We really should switch on major type, but it is not an enum or ordinal.
   * @return true if the value was successfully set.
   */
  protected static boolean setSafe(ValueVector vector, int index, Object value) {
    switch (vector.getField().getType().getMinorType()) {
    case INT:       return setSafe((IntVector)vector, index, (int)value);
    case VARCHAR:   return setSafe((VarCharVector)vector, index, (String)value);
    default:        return false;
    }
  }

  
  /**
   * Strongly typed routines for setting a Java value into a value vector. 
   * @return true if the value was successfully set.
   */
  protected static boolean setSafe(VarCharVector v, int index, String string) {
    return v.getMutator().setSafe(index, string.getBytes(UTF8));
  } 
  
  protected static boolean setSafe(IntVector v, int index, int value) {
    return v.getMutator().setSafe(index, value);
  }
   
}
