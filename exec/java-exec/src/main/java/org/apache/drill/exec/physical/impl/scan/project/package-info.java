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
 * Provides run-time semantic analysis of the projection list for the
 * scan operator. The project list can include table columns and a
 * variety of special columns. Requested columns can exist in the table,
 * or may be "missing" with null values applied. The code here prepares
 * a run-time projection plan based on the actual table schema.
 * <p>
 * Looks at schema as a set of transforms.
 * <ul>
 * <li>Scan-level projection list from the query plan: The list of columns
 * (or the wildcard) as requested by the user in the query. The planner
 * determines which columns to project. In Drill, projection is speculative:
 * it is a list of names which the planner hopes will appear in the data
 * files. The reader must make up columns (the infamous nullable INT) when
 * it turns out that no such column exists. Else, the reader must figure out
 * the data type for any columns that does exist.
 * <p>
 * The scan project list defines the set of columns which the scan operator
 * is obliged to send downstream. Ideally, the scan operator sends exactly the
 * same schema (the project list with types filled in) for all batches. Since
 * batches may come from different files, the scan operator is obligated to
 * unify the schemas from those files (or blocks.)</ul>
 * <li>Reader (file)-level projection occurs for each reader. A single scan
 * may use multiple readers to read data. Each reader may offer more information
 * about the schema. For example, a Parquet reader can obtain schema information
 * from the Parquet headers. A JDBC reader obtains schema information from the
 * returned schema. This is called "early schema." File-based readers can at least
 * add implicit file or partition columns.
 * <p>
 * The result is a refined schema: the scan level schema with more information
 * filled in. For Parquet, all projection information can be filled in. For
 * CSV or JSON, we can only add file metadata information, but not yet the
 * actual data schema.</ul>
 * <li>Batch-level schema: once a reader reads actual data, it now knows
 * exactly what it read. This is the "schema on read model." Thus, after reading
 * a batch, any remaining uncertainty about the projected schema is removed.
 * The actual data defined data types and so on.
 * <p>
 * Readers such as JSON and CSV are "late schema": they don't know the data
 * schema until they read the file. This is true "schema on read." Further, for
 * JSON, the data may change from one batch to the next as the reader "discovers"
 * fields that did not appear in earlier batches. This requires some amount of
 * "schema smoothing": the ability to preserve a consistent output schema even
 * as the input schema jiggles around some.</ul>
 * </ul>
 * <p>
 * The goal of this mechanism is to handle the above use cases cleanly, in a
 * common set of classes, and to avoid the need for each reader to figure out
 * all these issues for themselves (as was the case with earlier versions of
 * Drill.)
 * <p>
 * Because these issues are complex, the code itself is complex. To make the
 * code easier to manage, each bit of functionality is encapsulated in a
 * distinct class. Classes combine via composition to create a "framework"
 * suitable for each kind of reader: whether it be early or late schema,
 * file-based or something else, etc.
 * <p>
 * The core concept is one of successive refinement of the project
 * list through a set of rewrites:
 * <ul>
 * <li>Scan-level rewrite: convert {@link SchemaPath} entries into
 * internal column nodes, tagging the nodes with the column type:
 * wildcard, unresolved table column, or special columns (such as
 * file metadata.) The scan-level rewrite is done once per scan
 * operator.</li>
 * <li>Reader-level rewrite: convert the internal column nodes into
 * other internal nodes, leaving table column nodes unresolved. The
 * typical use is to fill in metadata columns with information about a
 * specific file.</li>
 * <li>Schema-level rewrite: given the actual schema of a record batch,
 * rewrite the reader-level projection to describe the final projection
 * from incoming data to output container. This step fills in missing
 * columns, expands wildcards, etc.</li>
 * </ul>
 * The following outlines the steps from scan plan to per-file data
 * loading to producing the output batch. The center path is the
 * projection metadata which turns into an actual output batch.
 * <pre>
 *                   Scan Plan
 *                       |
 *                       v
 *               +--------------+
 *               | Project List |
 *               |    Parser    |
 *               +--------------+
 *                       |
 *                       v
 *                +------------+
 *                | Scan Level |
 *                | Projection | -----------+
 *                +------------+            |
 *                       |                  |
 *                       v                  v
 *  +------+      +------------+     +------------+      +-----------+
 *  | File | ---> | File Level |     | Result Set | ---> | Data File |
 *  | Data |      | Projection |     |   Loader   | <--- |  Reader   |
 *  +------+      +------------+     +------------+      +-----------+
 *                       |                  |
 *                       v                  |
 *               +--------------+   Table   |
 *               | Schema Level |   Schema  |
 *               |  Projection  | <---------+
 *               +--------------+           |
 *                       |                  |
 *                       v                  |
 *                  +--------+   Loaded     |
 *                  | Output |   Vectors    |
 *                  | Mapper | <------------+
 *                  +--------+
 *                       |
 *                       v
 *                 Output Batch
 * </pre>
 * <p>
 * The output mapper includes mechanisms to populate implicit columns, create
 * null columns, and to merge implicit, null and data columns, omitting
 * unprojected data columns.
 * <p>
 * In all cases, projection must handle maps, which are a recursive structure
 * much like a row. That is, Drill consists of nested tuples (the row and maps),
 * each of which contains columns which can be maps. Thus, there is a set of
 * alternating layers of tuples, columns, tuples, and so on until we get to leaf
 * (non-struct) columns. As a result, most of the above structures are in the form
 * of tuple trees, requiring recursive algorithms to apply rules down through the
 * nested layers of tuples.
 * <p>
 * The above mechanism is done at runtime, in each scan fragment. Since Drill is
 * schema-on-read, and has no plan-time schema concept, run-time projection is
 * required. On the other hand, if Drill were ever to support the "classic"
 * plan-time schema resolution, then much of this work could be done at plan
 * time rather than (redundantly) at runtime. The main change would be to do
 * the work abstractly, working with column and row descriptions, rather than
 * concretely with vectors as is done here. Then, that abstract description
 * would feed directly into these mechanisms with the "final answer" about
 * projection, batch layout, and so on. The parts of this mechanism that
 * create and populate vectors would remain.
 */

package org.apache.drill.exec.physical.impl.scan.project;
