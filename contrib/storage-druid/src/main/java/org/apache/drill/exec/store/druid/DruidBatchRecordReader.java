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

package org.apache.drill.exec.store.druid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.store.druid.DruidSubScan.DruidSubScanSpec;
import org.apache.drill.exec.store.druid.common.DruidFilter;
import org.apache.drill.exec.store.druid.druid.DruidScanResponse;
import org.apache.drill.exec.store.druid.druid.ScanQuery;
import org.apache.drill.exec.store.druid.druid.ScanQueryBuilder;
import org.apache.drill.exec.store.druid.rest.DruidQueryClient;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.loader.JsonLoaderImpl.JsonLoaderBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class DruidBatchRecordReader implements ManagedReader<SchemaNegotiator> {
  private static final Logger logger = LoggerFactory.getLogger(DruidBatchRecordReader.class);
  private static final int BATCH_SIZE = 4096;
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private final DruidStoragePlugin plugin;
  private final DruidSubScan.DruidSubScanSpec scanSpec;
  private final List<String> columns;
  private final DruidFilter filter;
  private final DruidQueryClient druidQueryClient;
  private final DruidOffsetTracker offsetTracker;
  private int maxRecordsToRead = -1;
  private JsonLoaderBuilder jsonBuilder;
  private JsonLoaderImpl jsonLoader;
  private SchemaNegotiator negotiator;
  private ResultSetLoader resultSetLoader;
  private CustomErrorContext errorContext;


  public DruidBatchRecordReader(DruidSubScan subScan,
                                DruidSubScanSpec subScanSpec,
                                List<SchemaPath> projectedColumns,
                                int maxRecordsToRead,
                                DruidStoragePlugin plugin, DruidOffsetTracker offsetTracker) {
    this.columns = new ArrayList<>();
    this.maxRecordsToRead = maxRecordsToRead;
    this.plugin = plugin;
    this.scanSpec = subScanSpec;
    this.filter = subScanSpec.getFilter();
    this.druidQueryClient = plugin.getDruidQueryClient();
    this.offsetTracker = offsetTracker;
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    this.negotiator = negotiator;
    this.errorContext = this.negotiator.parentErrorContext();
    this.negotiator.batchSize(BATCH_SIZE);
    this.negotiator.setErrorContext(errorContext);

    resultSetLoader = this.negotiator.build();


    return true;
  }

  @Override
  public boolean next() {
    jsonBuilder = new JsonLoaderBuilder()
        .resultSetLoader(resultSetLoader)
        .standardOptions(negotiator.queryOptions())
        .errorContext(errorContext);
    int eventCounter = 0;
    boolean result = false;
    try {
      String query = getQuery();
      logger.debug("Executing query: {}", query);
      DruidScanResponse druidScanResponse = druidQueryClient.executeQuery(query);
      setNextOffset(druidScanResponse);

      StringBuilder events = new StringBuilder();
      for (ObjectNode eventNode : druidScanResponse.getEvents()) {
        events.append(eventNode);
        events.append("\n");
        eventCounter++;
      }


      jsonLoader = (JsonLoaderImpl) jsonBuilder
          .fromString(events.toString())
          .build();

      result = jsonLoader.readBatch();

      if (eventCounter < BATCH_SIZE) {
        return false;
      } else {
        return result;
      }
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failure while executing druid query: " + e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }
  }

  @Override
  public void close() {
    if (jsonLoader != null) {
      jsonLoader.close();
      jsonLoader = null;
    }
  }

  private String getQuery() throws JsonProcessingException {
    int queryThreshold =
      maxRecordsToRead >= 0
        ? Math.min(BATCH_SIZE, maxRecordsToRead)
        : BATCH_SIZE;
    ScanQueryBuilder scanQueryBuilder = plugin.getScanQueryBuilder();
    ScanQuery scanQuery =
      scanQueryBuilder.build(
        scanSpec.dataSourceName,
        columns,
        filter,
        offsetTracker.getOffset(),
        queryThreshold,
        scanSpec.getMinTime(),
        scanSpec.getMaxTime()
      );
    return objectMapper.writeValueAsString(scanQuery);
  }

  private void setNextOffset(DruidScanResponse druidScanResponse) {
    offsetTracker.setNextOffset(BigInteger.valueOf(druidScanResponse.getEvents().size()));
  }
}
