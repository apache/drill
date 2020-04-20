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
package org.apache.drill.exec.physical.impl.scan.v3.lifecycle;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.RowBatchReader;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.ScanLifecycleBuilder;
import org.apache.drill.exec.physical.impl.scan.v3.SchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader.EarlyEofException;
import org.apache.drill.exec.physical.impl.scan.v3.lifecycle.OutputBatchBuilder.BatchSource;
import org.apache.drill.exec.physical.impl.scan.v3.schema.ScanSchemaTracker;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetOptionBuilder;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the schema and batch construction for a managed reader.
 * Allows the reader itself to be as simple as possible. This class
 * implements the basic {@link RowBatchReader} protocol based on
 * three methods, and converts it to the two-method protocol of
 * the managed reader. The {@code open()} call of the
 * {@code RowBatchReader} is combined with the constructor of the
 * {@link ManagedReader}, enforcing the rule that the managed reader
 * is created just-in-time when it is to be used, which avoids
 * accidentally holding resources for the life of the scan.
 * <p>
 * Coordinates the components that wrap a reader to create the final
 * output batch:
 * <ul>
 * <li>The actual reader which load (possibly a subset of) the
 * columns requested from the input source.</li>
 * <li>Implicit columns manager instance which populates implicit
 * file columns, partition columns, and Drill's internal metadata
 * columns.</li>
 * <li>The missing columns handler which "makes up" values for projected
 * columns not read by the reader.</li>
 * <li>Batch assembler, which combines the three sources of vectors
 * to create the output batch with the schema specified by the
 * schema tracker.</li>
 * </ul>
 * <p>
 * This class coordinates the reader-visible aspects of the scan:
 * <ul>
 * <li>The {@link SchemaNegotiator} (or subclass) which provides
 * schema-related input to the reader and which creates the reader's
 * {@link ResultSetLoader}, among other tasks. The schema negotiator
 * is specific to each kind of scan and is thus created via the
 * {@link ScanLifecycleBuilder}.</li>
 * <li>The reader, which is designed to be as simple as possible,
 * with all generic overhead tasks handled by this "shim" between
 * the scan operator and the actual reader implementation.</li>
 * </ul>
 * <p>
 * The reader is schema-driven. See {@link ScanSchemaTracker} for
 * an overview.
 * <ul>
 * <li>The reader is given a <i>reader input schema</i>, via the
 * schema negotiator, which specifies the desired output schema.
 * The schema can be fully dynamic (a wildcard), fully defined (a
 * prior reader already chose column types), or a hybrid.</li>
 * <li>The reader can load a subset of columns. Those that are
 * left out become "missing columns" to be filled in by this
 * class.</li>
 * <li>The <i>reader output schema</i> along with implicit and missing
 * columns, together define the scan's output schema.</li>
 * </ul>
 * <p>
 * The framework handles the projection task so the
 * reader does not have to worry about it. Reading an unwanted column
 * is low cost: the result set loader will have provided a "dummy" column
 * writer that simply discards the value. This is just as fast as having the
 * reader use if-statements or a table to determine which columns to save.
 */
public class ReaderLifecycle implements RowBatchReader {
  private static final Logger logger = LoggerFactory.getLogger(ReaderLifecycle.class);

  private final ScanLifecycle scanLifecycle;
  protected final TupleMetadata readerInputSchema;
  private ManagedReader reader;
  private final SchemaNegotiatorImpl schemaNegotiator;
  protected ResultSetLoader tableLoader;
  private int prevTableSchemaVersion;
  private StaticBatchBuilder implicitColumnsLoader;
  private StaticBatchBuilder missingColumnsHandler;
  private OutputBatchBuilder outputBuilder;

  /**
   * True once the reader reports EOF. This shim may keep going for another
   * batch to handle any look-ahead row on the last batch.
   */
  private boolean eof;

  public ReaderLifecycle(ScanLifecycle scanLifecycle) {
    this.scanLifecycle = scanLifecycle;
    this.readerInputSchema = schemaTracker().readerInputSchema();
    this.schemaNegotiator = scanLifecycle.newNegotiator(this);
  }

  public ScanLifecycle scanLifecycle() { return scanLifecycle; }
  public TupleMetadata readerInputSchema() { return readerInputSchema; }
  public CustomErrorContext errorContext() { return schemaNegotiator.errorContext(); }

  public ScanSchemaTracker schemaTracker() {
    return scanLifecycle.schemaTracker();
  }

  public ScanLifecycleBuilder scanOptions() {
    return scanLifecycle.options();
  }

  @Override
  public String name() {
    return reader.getClass().getSimpleName();
  }

  @Override
  public boolean open() {
    try {
      reader = schemaNegotiator.newReader(scanLifecycle.readerFactory());
    } catch (EarlyEofException e) {
      logger.info("Reader has no data or schema, skipped. Factory: {}",
          scanLifecycle.readerFactory().getClass().getSimpleName());
      reader = null;
      return false;
    } catch (UserException e) {
      throw e;
    } catch (Exception e) {
      throw UserException.dataReadError(e)
        .addContext("Failed to open reader")
        .addContext(errorContext())
        .build(logger);
    }

    // Storage plugins are extensible: a novice developer may not
    // have known to create the table loader. Fail in this case.
    if (tableLoader == null) {
      throw UserException.internalError()
        .message("Reader returned true from open, but did not call SchemaNegotiator.build().")
        .addContext("Reader", reader.getClass().getSimpleName())
        .addContext(errorContext())
        .build(logger);
    }
    return true;
  }

  public ResultSetLoader buildLoader() {
    ResultSetOptionBuilder options = new ResultSetOptionBuilder()
        .rowCountLimit(Math.min(schemaNegotiator.batchSize, scanOptions().scanBatchRecordLimit()))
        .vectorCache(scanLifecycle.vectorCache())
        .batchSizeLimit(scanOptions().scanBatchByteLimit())
        .errorContext(errorContext())
        .projectionFilter(schemaTracker().projectionFilter(errorContext()))
        .readerSchema(schemaNegotiator.readerSchema);

    // Resolve the scan scame if possible.
    applyEarlySchema();

    // Create the table loader
    tableLoader = new ResultSetLoaderImpl(scanLifecycle.allocator(), options.build());
    implicitColumnsLoader = schemaNegotiator.implicitColumnsLoader();
    return tableLoader;
  }

  /**
   * If schema is complete, update the output schema. The reader schema provided
   * by the negotiator contains all the columns the reader could provide and is
   * not adjusted for projection. The schema tracker will apply the same projection
   * filter as used when loading data. As usual, the reader may not provided all
   * projected columns, so we apply missing columns early to produce a fully-resolved
   * scan schema.
   */
  private void applyEarlySchema() {
    if (schemaNegotiator.isSchemaComplete()) {
      schemaTracker().applyEarlyReaderSchema(schemaNegotiator.readerSchema);
      TupleMetadata missingCols = missingColumnsBuilder(schemaNegotiator.readerSchema).buildSchema();
      if (missingCols != null) {
        schemaTracker().resolveMissingCols(missingCols);
      }
    }
  }

  @Override
  public boolean defineSchema() {
    if (!schemaNegotiator.isSchemaComplete()) {
      return false;
    }
    tableLoader.startBatch();
    endBatch();
    return true;
  }

  @Override
  public boolean next() {

    // The reader may report EOF, but the result set loader might
    // have a lookahead row.
    if (isEof()) {
      return false;
    }

    // Prepare for the batch.
    tableLoader.startBatch();

    // Read the batch. The reader should report EOF if it hits the
    // end of data, even if the reader returns rows. This will prevent allocating
    // a new batch just to learn about EOF. Don't read if the reader
    // already reported EOF. In that case, we're just processing any last
    // lookahead row in the result set loader.
    if (!eof) {
      try {
        eof = !reader.next();
      } catch (UserException e) {
        throw e;
      } catch (Exception e) {
        throw UserException.dataReadError(e)
          .addContext("File read failed")
          .addContext(errorContext())
          .build(logger);
      }
    }

    // Add implicit columns, if any.
    // Identify the output container and its schema version.
    // Having a correct row count, even if 0, is important to
    // the scan operator.
    endBatch();

    // Return EOF (false) only when the reader reports EOF
    // and the result set loader has drained its rows from either
    // this batch or lookahead rows.
    return !isEof();
  }

  public boolean isEof() {
    return eof && !tableLoader.hasRows();
  }

  /**
   * Build the final output batch by projecting columns from the three input sources
   * to the output batch. First, build the metadata and/or null columns for the
   * table row count. Then, merge the sources.
   */
  private void endBatch() {
    VectorContainer readerOutput = tableLoader.harvest();

    // Corner case: Reader produced no rows and defined no schema.
    // We assume this is a null file and skip it as if we got an
    // EOF from the constructor. There may be some actual case in
    // which a file is known to have no records and no columns,
    // which is an empty file. At present, there is no way to differentiate
    // an intentional empty file from a null file.
    if (readerOutput.getRecordCount() == 0 && tableLoader.schemaVersion() == 0) {
      readerOutput.clear();
      return;
    }

    // If not the first batch, and there are no rows, discard the batch
    // as it adds no new information.
    if (readerOutput.getRecordCount() == 0 && tableLoader.batchCount() > 1) {
      readerOutput.clear();
      outputBuilder = null;
      return;
    }

    // If the schema changed, set up the final projection based on
    // the new (or first) schema.
    if (tableLoader.batchCount() == 1 || prevTableSchemaVersion < tableLoader.schemaVersion()) {
      reviseOutputProjection(tableLoader.outputSchema());
    }
    buildOutputBatch(readerOutput);
  }

  private void reviseOutputProjection(TupleMetadata readerOutputSchema) {
    schemaTracker().applyReaderSchema(readerOutputSchema, schemaNegotiator.errorContext());
    missingColumnsHandler = missingColumnsBuilder(readerOutputSchema).build();
    if (missingColumnsHandler != null) {
      schemaTracker().resolveMissingCols(missingColumnsHandler.schema());
    }
    outputBuilder = null;
    prevTableSchemaVersion = tableLoader.schemaVersion();
  }

  public MissingColumnHandlerBuilder missingColumnsBuilder(TupleMetadata readerSchema) {
    return new MissingColumnHandlerBuilder()
        .allowRequiredNullColumns(scanOptions().allowRequiredNullColumns())
        .inputSchema(schemaTracker().missingColumns(readerSchema))
        .vectorCache(scanLifecycle.vectorCache())
        .nullType(scanOptions().nullType());
  }

  private void buildOutputBatch(VectorContainer readerContainer) {

    // Get the batch results in a container.
    int rowCount = readerContainer.getRecordCount();
    if (implicitColumnsLoader != null) {
      implicitColumnsLoader.load(rowCount);
    }
    if (missingColumnsHandler != null) {
      missingColumnsHandler.load(rowCount);
    }

    if (outputBuilder == null) {
      createOutputBuilder();
    }
    outputBuilder.load(rowCount);
  }

  private void createOutputBuilder() {
    List<BatchSource> sources = new ArrayList<>();
    sources.add(new BatchSource(tableLoader.outputSchema(), tableLoader.outputContainer()));
    if (implicitColumnsLoader != null) {
      sources.add(new BatchSource(implicitColumnsLoader.schema(), implicitColumnsLoader.outputContainer()));
    }
    if (missingColumnsHandler != null) {
      sources.add(new BatchSource(missingColumnsHandler.schema(), missingColumnsHandler.outputContainer()));
    }
    outputBuilder = new OutputBatchBuilder(schemaTracker().outputSchema(), sources,
        scanLifecycle.allocator());
  }

  public TupleMetadata readerOutputSchema() {
    return tableLoader == null ? null : tableLoader.outputSchema();
  }

  @Override
  public VectorContainer output() {
    return outputBuilder == null ? null : outputBuilder.outputContainer();
  }

  @Override
  public int schemaVersion() {
    return tableLoader.schemaVersion();
  }

  @Override
  public void close() {
    try {
      if (reader != null) {
        reader.close();
      }
    } catch (UserException e) {

      // Reader threw a user exception; assume that this is serious and must
      // kill the query.
      throw e;
    } catch (Exception e) {
      throw UserException.dataReadError(e)
        .addContext("Reader close failed")
        .addContext(errorContext())
        .build(logger);
    } finally {
      reader = null;
      if (tableLoader != null) {
        tableLoader.close();
        tableLoader = null;
      }
    }
  }
}
