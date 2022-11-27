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

package org.apache.drill.exec.store.http;


import okhttp3.HttpUrl;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.ChildErrorContext;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.v3.FixedReceiver;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.ImplicitColumnUtils.ImplicitColumns;
import org.apache.drill.exec.store.http.paginator.Paginator;
import org.apache.drill.exec.store.http.util.SimpleHttp;
import org.apache.drill.exec.store.xml.XMLReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.InputStream;

public class HttpXMLBatchReader extends HttpBatchReader {
  private static final Logger logger = LoggerFactory.getLogger(HttpXMLBatchReader.class);

  private final HttpSubScan subScan;
  private final int maxRecords;
  private final int dataLevel;
  private InputStream inStream;
  private XMLReader xmlReader;
  private ResultSetLoader resultLoader;

  public HttpXMLBatchReader(HttpSubScan subScan) {
    super(subScan);
    this.subScan = subScan;
    this.maxRecords = subScan.maxRecords();

    // TODO Remove the XMLDataLevel parameter.  For now, check both
    if (subScan.tableSpec().connectionConfig().xmlOptions() == null) {
      this.dataLevel = subScan.tableSpec().connectionConfig().xmlDataLevel();
    } else {
      this.dataLevel = subScan.tableSpec().connectionConfig().xmlOptions().getDataLevel();
    }
  }


  public HttpXMLBatchReader(HttpSubScan subScan, Paginator paginator) {
    super(subScan, paginator);
    this.subScan = subScan;
    this.maxRecords = subScan.maxRecords();

    if (subScan.tableSpec().connectionConfig().xmlOptions() == null) {
      this.dataLevel = subScan.tableSpec().connectionConfig().xmlDataLevel();
    } else {
      this.dataLevel = subScan.tableSpec().connectionConfig().xmlOptions().getDataLevel();
    }
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {

    HttpUrl url = buildUrl();

    // Result set loader setup
    String tempDirPath = negotiator.drillConfig().getString(ExecConstants.DRILL_TMP_DIR);

    // Create user-friendly error context
    CustomErrorContext errorContext = new ChildErrorContext(negotiator.parentErrorContext()) {
      @Override
      public void addContext(UserException.Builder builder) {
        super.addContext(builder);
        builder.addContext("URL", url.toString());
      }
    };
    negotiator.setErrorContext(errorContext);

    // Http client setup
    SimpleHttp http = SimpleHttp.builder()
      .scanDefn(subScan)
      .url(url)
      .tempDir(new File(tempDirPath))
      .paginator(paginator)
      .proxyConfig(proxySettings(negotiator.drillConfig(), url))
      .errorContext(errorContext)
      .build();

    // Get the input stream
    inStream = http.getInputStream();
    // Initialize the XMLReader the reader
    try {
      // Add schema if provided
      TupleMetadata finalSchema = getSchema(negotiator);
      if (finalSchema != null) {
        negotiator.tableSchema(finalSchema, false);
      }

      xmlReader = new XMLReader(inStream, dataLevel);
      resultLoader = negotiator.build();

      if (implicitColumnsAreProjected()) {
        implicitColumns = new ImplicitColumns(resultLoader.writer());
        buildImplicitColumns();
        populateImplicitFieldMap(http);
      }

      RowSetLoader rootRowWriter = resultLoader.writer();
      xmlReader.open(rootRowWriter, errorContext);
      xmlReader.implicitFields(implicitColumns);
    } catch (XMLStreamException e) {
      throw UserException
        .dataReadError(e)
        .message("Error opening XML stream: " + e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }

    // Close cache resources
    http.close();
    return true;
  }

  /**
   * This function obtains the correct schema for the {@link XMLReader}.  There are four possibilities:
   * 1.  The schema is provided in the configuration only.  In this case, that schema will be returned.
   * 2.  The schema is provided in both the configuration and inline.  These two schemas will be merged together.
   * 3.  The schema is provided inline in a query.  In this case, that schema will be returned.
   * 4.  No schema is provided.  Function returns null.
   * @param negotiator {@link SchemaNegotiator} The schema negotiator with all the connection information
   * @return The built {@link TupleMetadata} of the provided schema, null if none provided.
   */
  private TupleMetadata getSchema(SchemaNegotiator negotiator) {
    if (subScan.tableSpec().connectionConfig().xmlOptions() != null &&
      subScan.tableSpec().connectionConfig().xmlOptions().schema() != null) {
      TupleMetadata configuredSchema = subScan.tableSpec().connectionConfig().xmlOptions().schema();

      // If it has a provided schema both inline and in the config, merge the two, otherwise, return the config schema
      if (negotiator.hasProvidedSchema()) {
        TupleMetadata inlineSchema = negotiator.providedSchema();
        return FixedReceiver.Builder.mergeSchemas(configuredSchema, inlineSchema);
      } else {
        return configuredSchema;
      }
    } else {
      if (negotiator.hasProvidedSchema()) {
        return negotiator.providedSchema();
      }
    }
    return null;
  }


  @Override
  public boolean next() {
    boolean result;
    try {
      result = xmlReader.next();
    } catch (UserException e) {
      // This covers the case of an empty XML response.  We don't want to throw an
      // exception, just catch anything and halt execution.  Otherwise, throw the original exception.
      if (e.getMessage().contains("EOF")) {
        return false;
      } else {
        throw e;
      }
    }

    if (paginator != null &&
      maxRecords < 0 && (resultLoader.totalRowCount()) < paginator.getPageSize()) {
      paginator.notifyPartialPage();
    }

    return result;
  }

  @Override
  public void close() {
    AutoCloseables.closeSilently(xmlReader);
    AutoCloseables.closeSilently(inStream);
  }
}
