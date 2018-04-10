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
 * Implementation of the vector writers. The code will make much more sense if
 * we start with a review of Drill’s complex vector data model. Drill has 38+
 * data (“minor”) types. Drill also has three cardinalities (“modes”). The
 * result is over 120+ different vector types. Then, when you add maps, repeated
 * maps, lists and repeated lists, you rapidly get an explosion of types that
 * the writer code must handle.
 *
 * <h4>Understanding the Vector Model</h4>
 *
 * Vectors can be categorized along multiple dimensions:
 * <ul>
 * <li>By data (minor) type</li>
 * <li>By cardinality (mode)</li>
 * <li>By fixed or variable width</li>
 * <li>By repeat levels</li>
 * </ul>
 * <p>
 * A repeated map, a list, a repeated list and any array (repeated) scalar all
 * are array-like. Nullable and required modes are identical (single values),
 * but a nullable has an additional is-set (“bit”) vector.
 * <p>
 * The writers (and readers) borrow concepts from JSON and relational theory
 * to simplify the problem:
 * <p>
 * <ul>
 * <li>Both the top-level row, and a Drill map are “tuples” and are treated
 * similarly in the model.</li>
 * <li>All non-map, non-list (that is, scalar) data types are treated
 * uniformly.</li>
 * <li>All arrays (whether a list, a repeated list, a repeated map, or a
 * repeated scalar) are treated uniformly.</li>
 * </ul>
 *
 * <h4>Repeat Levels</h4>
 *
 * JSON and Parquet can be understood as a series of one or more "repeat
 * levels." First, let's identify the repeat levels above the batch
 * level:
 * <ul>
 * <li>The top-most level is the "result set": the entire collection of
 * rows that come from a file (or other data source.)</li>
 * <li>Result sets are divided into batches: collections of up to 64K
 * rows.</li>
 * </ul>
 *
 * Then, within a batch:
 * <ul>
 * <li>Each batch is a collection or rows. A batch-level index points
 * to the current row.</li>
 * </ul>Scalar arrays introduce a repeat level: each row has 0, 1 or
 * many elements in the array-valued column. An offset vector indexes
 * to the first value for each row. Each scalar array has its own
 * per-array index to point to the next write position.</li>
 * <li>Map arrays introduce a repeat level for a group of columns
 * (those that make up the map.) A single offset vector points to
 * the common start position for the columns. A common index points
 * to the common next write position.<li>
 * <li>Lists also introduce a repeat level. (Details to be worked
 * out.</li>
 * </ul>
 *
 * For repeated vectors, one can think of the structure either top-down
 * or bottom-up:
 * <ul>
 * <li>Top down: the row position points into an offset vector. The
 * offset vector value points to either the data value, or into another
 * offset vector.</li>
 * <li>Bottom-up: values are appended to the end of the vector. Values
 * are "pinched off" to form an array (for repeated maps) or for a row.
 * In this view, indexes bubble upward. The inner-most last write position
 * is written as the array end position in the enclosing offset vector.
 * This may occur up several levels.</li>
 * </ul>
 *
 * <h4>Writer Data Model</h4>
 *
 * The above leads to a very simple, JSON-like data model:
 * <ul>
 * <li>A tuple reader or writer models a row. (Usually via a subclass.) Column
 * are accessible by name or position.</li>
 * <li>Every column is modeled as an object.</li>
 * <li>The object can have an object type: scalar, tuple or array.</li>
 * <li>An array has a single element type (but many run-time elements)</li>
 * <li>A scalar can be nullable or not, and provides a uniform get/set
 * interface.</li>
 * </ul>
 * <p>
 * This data model is similar to; but has important differences from, the prior,
 * generated, readers and writers.
 * <p>
 * The object layer is new: it is the simplest way to model the three “object
 * types.” An app using this code would use just the leaf scalar readers and
 * writers.
 *
 * <h4>Writer Performance</h4>
 *
 * To maximize performance, have a single version for all "data modes":
 * (nullable, required, repeated). Some items of note:
 * <ul>
 * <li>The writers bypass DrillBuf and the UDLE to needed writes to direct
 * memory.</li>
 * <li>The writers buffer the buffer address and implement a number of methods
 * to synchronize that address when the buffer changes (on a new batch or during
 * vector resize).</li>
 * <li>Writing require a single bounds check. In most cases, the write is within
 * bounds so the single check is all that is needed.</li>
 * <li>If the write is out of bounds, then the writer determines the new vector
 * size and performs the needed reallocation. To avoid multiple doublings, the
 * writer computes the needed new size and allocates that size directly.</li>
 * <li>Vector reallocation is improved to eliminate zeroing the new half of the
 * buffer, data is left “garbage-filled.”</li>
 * <li>If the vector would grow beyond 16 MB, then overflow is triggered, via a
 * listener, which causes the buffer to be replaced. The write then
 * continues.</li>
 * <li>Offset vector updates are integrated into the writers using an
 * `OffsetVectorWriter`. This writer caches the last write position so that each
 * array write needs a single offset update, rather than the read and write as
 * in previous code.</li>
 * <li>The writers keep track of the “last write position” and perform
 * “fill-empties” work if the new write position is more than one position
 * behind the last write. All types now correctly support “fill-empties”
 * (before, only nullable types did so reliably.)</li>
 * <li>Null handling is done by an additional writer layer that wraps the
 * underlying data writer. This avoids the need for a special nullable writer:
 * the same nullable layer works for all data types.</li>
 * <li>Array handling is done similarly: an array writer manages the offset
 * vector and works the same for repeated scalars, repeated maps and
 * (eventually) lists and repeated lists.</li>
 * </ul>
 */

package org.apache.drill.exec.vector.accessor.writer;
