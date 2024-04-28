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

package org.apache.drill.exec.store.daffodil;

import org.apache.daffodil.japi.DataProcessor;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileDescrip;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.daffodil.schema.DaffodilDataProcessorFactory;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import static org.apache.drill.exec.store.daffodil.schema.DaffodilDataProcessorFactory.CompileFailure;
import static org.apache.drill.exec.store.daffodil.schema.DrillDaffodilSchemaUtils.daffodilDataProcessorToDrillSchema;

public class DaffodilBatchReader implements ManagedReader {

  private static final Logger logger = LoggerFactory.getLogger(DaffodilBatchReader.class);
  private final RowSetLoader rowSetLoader;
  private final CustomErrorContext errorContext;
  private final DaffodilMessageParser dafParser;
  private final InputStream dataInputStream;

  public DaffodilBatchReader(DaffodilReaderConfig readerConfig, EasySubScan scan,
      FileSchemaNegotiator negotiator) {

    errorContext = negotiator.parentErrorContext();
    DaffodilFormatConfig dafConfig = readerConfig.plugin.getConfig();

    String schemaURIString = dafConfig.getSchemaURI(); // "schema/complexArray1.dfdl.xsd";
    String rootName = dafConfig.getRootName();
    String rootNamespace = dafConfig.getRootNamespace();
    boolean validationMode = dafConfig.getValidationMode();

    URI dfdlSchemaURI;
    try {
      dfdlSchemaURI = new URI(schemaURIString);
    } catch (URISyntaxException e) {
      throw UserException.validationError(e).build(logger);
    }

    FileDescrip file = negotiator.file();
    DrillFileSystem fs = file.fileSystem();
    URI fsSchemaURI = fs.getUri().resolve(dfdlSchemaURI);

    DaffodilDataProcessorFactory dpf = new DaffodilDataProcessorFactory();
    DataProcessor dp;
    try {
      dp = dpf.getDataProcessor(fsSchemaURI, validationMode, rootName, rootNamespace);
    } catch (CompileFailure e) {
      throw UserException.dataReadError(e)
          .message(String.format("Failed to get Daffodil DFDL processor for: %s", fsSchemaURI))
          .addContext(errorContext).addContext(e.getMessage()).build(logger);
    }
    // Create the corresponding Drill schema.
    // Note: this could be a very large schema. Think of a large complex RDBMS schema,
    // all of it, hundreds of tables, but all part of the same metadata tree.
    TupleMetadata drillSchema = daffodilDataProcessorToDrillSchema(dp);
    // Inform Drill about the schema
    negotiator.tableSchema(drillSchema, true);

    //
    // DATA TIME: Next we construct the runtime objects, and open files.
    //
    // We get the DaffodilMessageParser, which is a stateful driver for daffodil that
    // actually does the parsing.
    rowSetLoader = negotiator.build().writer();

    // We construct the Daffodil InfosetOutputter which the daffodil parser uses to
    // convert infoset event calls to fill in a Drill row via a rowSetLoader.
    DaffodilDrillInfosetOutputter outputter = new DaffodilDrillInfosetOutputter(rowSetLoader);

    // Now we can set up the dafParser with the outputter it will drive with
    // the parser-produced infoset.
    dafParser = new DaffodilMessageParser(dp); // needs further initialization after this.
    dafParser.setInfosetOutputter(outputter);

    Path dataPath = file.split().getPath();
    // Lastly, we open the data stream
    try {
      dataInputStream = fs.openPossiblyCompressedStream(dataPath);
    } catch (IOException e) {
      throw UserException.dataReadError(e)
          .message(String.format("Failed to open input file: %s", dataPath.toString()))
          .addContext(errorContext).addContext(e.getMessage()).build(logger);
    }
    // And lastly,... tell daffodil the input data stream.
    dafParser.setInputStream(dataInputStream);
  }

  /**
   * This is the core of actual processing - data movement from Daffodil to Drill.
   * <p>
   * If there is space in the batch, and there is data available to parse then this calls the
   * daffodil parser, which parses data, delivering it to the rowWriter by way of the infoset
   * outputter.
   * <p>
   * Repeats until the rowWriter is full (a batch is full), or there is no more data, or a parse
   * error ends execution with a throw.
   * <p>
   * Validation errors and other warnings are not errors and are logged but do not cause parsing to
   * fail/throw.
   *
   * @return true if there are rows retrieved, false if no rows were retrieved, which means no more
   *     will ever be retrieved (end of data).
   * @throws RuntimeException
   *     on parse errors.
   */
  @Override
  public boolean next() {
    // Check assumed invariants
    // We don't know if there is data or not. This could be called on an empty data file.
    // We DO know that this won't be called if there is no space in the batch for even 1
    // row.
    if (dafParser.isEOF()) {
      return false; // return without even checking for more rows or trying to parse.
    }
    while (rowSetLoader.start() && !dafParser.isEOF()) { // we never zero-trip this loop.
      // the predicate is always true once.
      dafParser.parse();
      if (dafParser.isProcessingError()) {
        assert (Objects.nonNull(dafParser.getDiagnostics()));
        throw UserException.dataReadError().message(dafParser.getDiagnosticsAsString())
            .addContext(errorContext).build(logger);
      }
      if (dafParser.isValidationError()) {
        logger.warn(dafParser.getDiagnosticsAsString());
        // Note that even if daffodil is set to not validate, validation errors may still occur
        // from DFDL's "recoverableError" assertions.
      }
      rowSetLoader.save();
    }
    int nRows = rowSetLoader.rowCount();
    assert nRows > 0; // This cannot be zero. If the parse failed we will have already thrown out
    // of here.
    return true;
  }

  @Override
  public void close() {
    AutoCloseables.closeSilently(dataInputStream);
  }
}

class DaffodilReaderConfig {
  final DaffodilFormatPlugin plugin;

  DaffodilReaderConfig(DaffodilFormatPlugin plugin) {
    this.plugin = plugin;
  }
}
