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
package org.apache.drill.exec.physical.impl.scan.framework;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.TupleMetadata;

/**
 * Negotiates the table schema with the scanner framework and provides
 * context information for the reader. In a typical scan, the physical
 * plan provides the project list: the set of columns that the query
 * expects. Readers provide a table schema: the set of columns actually
 * available. The scan framework combines the two lists to determine
 * the available table columns that must be read, along with any additional
 * to be added. Additional columns can be file metadata (if the storage
 * plugin requests them), or can be null columns added for projected
 * columns that don't actually exist in the table.
 * <p>
 * The reader provides the table schema in one of two ways:
 * <ul>
 * <li>If the reader is of "early schema" type, then the reader calls
 * {@link #setTableSchema(TupleMetadata)} to provide that schema.</li>
 * <li>If the reader is of "late schema" type, then the reader discovers
 * the schema as the data is read, calling the
 * {@link RowSetLoader#addColumn()} method to add each column as it is
 * discovered.
 * <p>
 * Either way, the project list from the physical plan determines which
 * table columns are materialized and which are not. Readers are provided
 * for all table columns for readers that must read sequentially, but
 * only the materialized columns are written to value vectors.
 * <p>
 * Regardless of the schema type, the result of building the schema is a
 * result set loader used to prepare batches for use in the query. The reader
 * can simply read all columns, allowing the framework to discard unwanted
 * values. Or for efficiency, the reader can check the column metadata to
 * determine if a column is projected, and if not, then don't even read
 * the column from the input source.
 */

public interface SchemaNegotiator {

  OperatorContext context();

  /**
   * Specify an advanced error context which allows the reader to
   * fill in custom context values.
   */

  void setErrorContext(CustomErrorContext context);

  /*
   * The name of the user running the query.
   */

  String userName();

  /**
   * Specify the table schema if this is an early-schema reader. Need
   * not be called for a late-schema readers. The schema provided here,
   * if any, is a base schema: the reader is free to discover additional
   * columns during the read.
   *
   * @param schema the table schema if known at open time
   * @param isComplete true if the schema is complete: if it can be used
   * to define an empty schema-only batch for the first reader. Set to
   * false if the schema is partial: if the reader must read rows to
   * determine the full schema
   */

  void setTableSchema(TupleMetadata schema, boolean isComplete);

  /**
   * Set the preferred batch size (which may be overridden by the
   * result set loader in order to limit vector or batch size.)
   *
   * @param maxRecordsPerBatch preferred number of record per batch
   */

  void setBatchSize(int maxRecordsPerBatch);

  /**
   * Build the schema, plan the required projections and static
   * columns and return a loader used to populate value vectors.
   * If the select list includes a subset of table columns, then
   * the loader will be set up in table schema order, but the unneeded
   * column loaders will be null, meaning that the batch reader should
   * skip setting those columns.
   *
   * @return the loader for the table with columns arranged in table
   * schema order
   */

  ResultSetLoader build();

  /**
   * Report whether the projection list is empty, as occurs in two
   * cases:
   * <ul>
   * <li><tt>SELECT COUNT(*) ...</tt> -- empty project.</ul>
   * <li><tt>SELECT a, b FROM table(c d)</tt> -- disjoint project.</li>
   * </ul>
   * @return true if no columns are projected, and the client can
   * make use of {@link ResultSetLoader#skipRows(int)} to indicate the
   * row count, false if at least one column is projected and so
   * data must be written using the loader
   */

  boolean isProjectionEmpty();

  /**
   * The context to use as a parent when creating a custom context.
   * <p>
   * (Obtain the error context for this reader from the
   * {@link ResultSetLoader}.
   */
  CustomErrorContext parentErrorContext();
}
