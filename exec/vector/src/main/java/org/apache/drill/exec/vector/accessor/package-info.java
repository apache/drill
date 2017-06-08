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
/**
 * Provides a light-weight, simplified set of column readers and writers that
 * can be plugged into a variety of row-level readers and writers. The classes
 * and interfaces here form a framework for accessing rows and columns, but do
 * not provide the code to build accessors for a given row batch. This code is
 * meant to be generic, but the first (and, thus far, only) use is with the test
 * framework for the java-exec project. That one implementation is specific to
 * unit tests, but the accessor framework could easily be used for other
 * purposes as well.
 * <p>
 * Drill provides a set of column readers and writers. Compared to those, this
 * set:
 * <ul>
 * <li>Works with all Drill data types. The other set works only with repeated
 * and nullable types.</li>
 * <li>Is a generic interface. The other set is bound tightly to the
 * {@link ScanBatch} class.</li>
 * <li>Uses generic types such as <tt>getInt()</tt> for most numeric types. The
 * other set has accessors specific to each of the ~30 data types which Drill
 * supports.</li>
 * </ul>
 * The key difference is that this set is designed for developer ease-of-use, a
 * primary requirement for unit tests. The other set is designed to be used in
 * machine-generated or write-once code and so can be much more complex.
 * <p>
 * That is, the accessors here are optimized for test code: they trade
 * convenience for a slight decrease in speed (the performance hit comes from
 * the extra level of indirection which hides the complex, type-specific code
 * otherwise required.)
 * <p>
 * {@link ColumnReader} and {@link ColumnWriter} are the core abstractions: they
 * provide simplified access to the myriad of Drill column types via a
 * simplified, uniform API. {@link TupleReader} and {@link TupleWriter} provide
 * a simplified API to rows or maps (both of which are tuples in Drill.)
 * {@link AccessorUtilities} provides a number of data conversion tools.
 * <p>
 * Overview of the code structure:
 * <dl>
 * <dt>TupleWriter, TupleReader</dt>
 * <dd>In relational terms, a tuple is an ordered collection of values, where
 * the meaning of the order is provided by a schema (usually a name/type pair.)
 * It turns out that Drill rows and maps are both tuples. The tuple classes
 * provide the means to work with a tuple: get the schema, get a column by name
 * or by position. Note that Drill code normally references columns by name.
 * But, doing so is slower than access by position (index). To provide efficient
 * code, the tuple classes assume that the implementation imposes a column
 * ordering which can be exposed via the indexes.</dd>
 * <dt>ColumnAccessor</dt>
 * <dd>A generic base class for column readers and writers that provides the
 * column data type.</dd>
 * <dt>ColumnWriter, ColumnReader</dt>
 * <dd>A uniform interface implemented for each column type ("major type" in
 * Drill terminology). The scalar types: Nullable (Drill optional) and
 * non-nullable (Drill required) fields use the same interface. Arrays (Drill
 * repeated) are special. To handle the array aspect, even array fields use the
 * same interface, but the <tt>getArray</tt> method returns another layer of
 * accessor (writer or reader) specific for arrays.
 * <p>
 * Both the column reader and writer use a reduced set of data types to access
 * values. Drill provides about 38 different types, but they can be mapped to a
 * smaller set for programmatic access. For example, the signed byte, short,
 * int; and the unsigned 8-bit, and 16-bit values can all be mapped to ints for
 * get/set. The result is a much simpler set of get/set methods compared to the
 * underlying set of vector types.</dt>
 * <dt>ArrayWriter, ArrayReader
 * <dt>
 * <dd>The interface for the array accessors as described above. Of particular
 * note is the difference in the form of the methods. The writer has only a
 * <tt>setInt()</tt> method, no index. The methods assume write-only, write-once
 * semantics: each set adds a new value. The reader, by contrast has a
 * <tt>getInt(int index)</tt> method: read access is random.</tt>
 * <dt>ScalarWriter<dt>
 * <dd>Because of the form of the array writer, both the array writer and
 * column writer have the same method signatures. To avoid repeating these
 * methods, they are factored out into the common <tt>ScalarWriter</tt>
 * interface.</dd>
 * <dt>ColumnAccessors (templates)</dt>
 * <dd>The Freemarker-based template used to generate the actual accessor
 * implementations.</dd>
 * <dt>ColumnAccessors (accessors)</dt>
 * <dd>The generated accessors: one for each combination of write/read, data
 * (minor) type and cardinality (data model).
 * <dd>
 * <dt>RowIndex</dt>
 * <dd>This nested class binds the accessor to the current row position for the
 * entire record batch. That is, you don't ask for the value of column a for row
 * 5, then the value of column b for row 5, etc. as with the "raw" vectors.
 * Instead, the implementation sets the row position (with, say an interator.)
 * Then, all columns implicitly return values for the current row.
 * <p>
 * Different implementations of the row index handle the case of no selection
 * vector, a selection vector 2, or a selection vector 4.</dd>
 * <dt>VectorAccessor</dt>
 * <dd>The readers can work with single batches or "hyper"
 * batches. A hyper batch occurs in operators such as sort where an operator
 * references a collection of batches as if they were one huge batch. In this
 * case, each column consists of a "stack" of vectors. The vector accessor picks
 * out one vector from the stack for each row. Vector accessors are used only
 * for hyper batches; single batches work directly with the corresponding
 * vector.
 * <p>
 * You can think of the (row index + vector accessor, column index) as forming a
 * coordinate pair. The row index provides the y index (vertical position along
 * the rows.) The vector accessor maps the row position to a vector when needed.
 * The column index picks out the x coordinate (horizontal position along the
 * columns.)</dt>
 * </dl>
 */

package org.apache.drill.exec.vector.accessor;
