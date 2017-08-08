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
package org.apache.drill.exec.physical.rowSet;

/**
 * Writes values into the current row or map by column index or name.
 * Column indexes and names are as defined by the schema.
 *
 * @see {@link SingleMapWriter}, the class which this class
 * replaces
 */

public interface TupleLoader {

  /**
   * Unchecked exception thrown when attempting to access a column loader
   * by name for an undefined columns. Readers that use a fixed schema
   * can simply omit catch blocks for the exception since it is unchecked
   * and won't be thrown if the schema can't evolve. Readers that can
   * discover new columns should catch the exception and define the
   * column.
   */

  @SuppressWarnings("serial")
  public static class UndefinedColumnException extends RuntimeException {
    public UndefinedColumnException(String msg) {
      super(msg);
    }
  }

  TupleSchema schema();
  ColumnLoader column(int colIndex);

  /**
   * Return the column loader for the given column name. Throws
   * the {@link UndefinedColumnException} exception if the column
   * is undefined. For readers, such as JSON, that work by name,
   * and discover columns as they appear on input,
   * first attempt to get the column loader. Catch the exception
   * if the column does not exist, define the column
   * then the column is undefined, and the code should add the
   * new column and again retrieve the loader.
   *
   * @param colName
   * @return the column loader for the column
   * @throws {@link UndefinedColumnException} if the column is
   * undefined.
   */
  ColumnLoader column(String colName);

  /**
   * Load a row using column values passed as variable-length arguments. Expects
   * map values to be flattened. a schema of (a:int, b:map(c:varchar)) would be>
   * set as <br><tt>loadRow(10, "foo");</tt><br> Values of arrays can be expressed as a Java
   * array. A schema of (a:int, b:int[]) can be set as<br>
   * <tt>loadRow(10, new int[] {100, 200});</tt><br>.
   * Primarily for testing, too slow for production code.
   * @param values column values in column index order
   * @return this loader
   */

  TupleLoader loadRow(Object...values);

  /**
   * Write a row that consists of a single object. Use this if Java becomes
   * confused about the whether the single argument to {@link #loadRow} is
   * an single array of values (what you want) or an array of multiple values
   * (which you don't want when setting an array.)
   *
   * @param value value of the single column to set
   * @return this loader
   */

  TupleLoader loadSingletonRow(Object value);

  /**
   * Write a value to the given column, automatically calling the proper
   * <tt>set<i>Type</i></tt> method for the data. While this method is
   * convenient for testing, it incurs quite a bit of type-checking overhead
   * and is not suitable for production code.
   * @param colIndex the index of the column to set
   * @param value the value to set. Must be of a type that maps to one of
   * the <tt>set<i>Type</i></tt> methods
   */

  void set(int colIndex, Object value);
}
