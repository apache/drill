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
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.druid.common.DruidFilter;
import org.apache.drill.exec.store.druid.druid.DruidSelectResponse;
import org.apache.drill.exec.store.druid.druid.PagingIdentifier;
import org.apache.drill.exec.store.druid.druid.PagingSpec;
import org.apache.drill.exec.store.druid.druid.SelectQuery;
import org.apache.drill.exec.store.druid.druid.SelectQueryBuilder;
import org.apache.drill.exec.store.druid.rest.DruidQueryClient;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DruidRecordReader extends AbstractRecordReader {

  private static final Logger logger = LoggerFactory.getLogger(DruidRecordReader.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private final DruidStoragePlugin plugin;
  private final DruidSubScan.DruidSubScanSpec scanSpec;
  private final List<String> dimensions;
  private final DruidFilter filter;
  private ArrayList<PagingIdentifier> pagingIdentifiers = new ArrayList<>();
  private int maxRecordsToRead = -1;

  private JsonReader jsonReader;
  private VectorContainerWriter writer;

  private final FragmentContext fragmentContext;
  private final DruidQueryClient druidQueryClient;

  public DruidRecordReader(DruidSubScan.DruidSubScanSpec subScanSpec,
                           List<SchemaPath> projectedColumns,
                           int maxRecordsToRead,
                           FragmentContext context,
                           DruidStoragePlugin plugin) {
    dimensions = new ArrayList<>();
    setColumns(projectedColumns);
    this.maxRecordsToRead = maxRecordsToRead;
    this.plugin = plugin;
    scanSpec = subScanSpec;
    fragmentContext = context;
    this.filter = subScanSpec.getFilter();
    this.druidQueryClient = plugin.getDruidQueryClient();
  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projectedColumns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    if (isStarQuery()) {
      transformed.add(SchemaPath.STAR_COLUMN);
    } else {
      for (SchemaPath column : projectedColumns) {
        String fieldName = column.getRootSegment().getPath();
        transformed.add(column);
        this.dimensions.add(fieldName);
      }
    }
    return transformed;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) {
    this.writer = new VectorContainerWriter(output);

    this.jsonReader =
      new JsonReader.Builder(fragmentContext.getManagedBuffer())
        .schemaPathColumns(ImmutableList.copyOf(getColumns()))
        .skipOuterList(true)
        .build();
  }

  @Override
  public int next() {
    writer.allocate();
    writer.reset();
    Stopwatch watch = Stopwatch.createStarted();
    try {
      String query = getQuery();
      DruidSelectResponse druidSelectResponse = druidQueryClient.executeQuery(query);
      setNextPagingIdentifiers(druidSelectResponse);

      int docCount = 0;
      for (ObjectNode eventNode : druidSelectResponse.getEvents()) {
        writer.setPosition(docCount);
        jsonReader.setSource(eventNode);
        try {
          jsonReader.write(writer);
        } catch (IOException e) {
          throw UserException
            .dataReadError(e)
            .message("Failure while reading document")
            .addContext("Failed Query", query)
            .addContext("Parser was at record", eventNode.toString())
            .addContext(e.getMessage())
            .build(logger);
        }
        docCount++;
      }

      writer.setValueCount(docCount);
      logger.debug("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), docCount);
      return docCount;
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failure while executing druid query")
        .addContext(e.getMessage())
        .build(logger);
    }
  }

  private String getQuery() throws JsonProcessingException {
    int queryThreshold =
      this.maxRecordsToRead >= 0
        ? Math.min(BaseValueVector.INITIAL_VALUE_ALLOCATION, this.maxRecordsToRead)
        : BaseValueVector.INITIAL_VALUE_ALLOCATION;
    SelectQueryBuilder selectQueryBuilder = plugin.getSelectQueryBuilder();
    SelectQuery selectQuery =
      selectQueryBuilder.build(
        scanSpec.dataSourceName,
        this.dimensions,
        this.filter,
        new PagingSpec(this.pagingIdentifiers, queryThreshold),
        scanSpec.getMinTime(),
        scanSpec.getMaxTime()
      );
    return objectMapper.writeValueAsString(selectQuery);
  }

  private void setNextPagingIdentifiers(DruidSelectResponse druidSelectResponse) {
    ArrayList<PagingIdentifier> newPagingIdentifiers = druidSelectResponse.getPagingIdentifiers();

    Map<String, String> newPagingIdentifierNames =
      newPagingIdentifiers
        .stream()
        .distinct()
        .collect(Collectors.toMap(PagingIdentifier::getSegmentName, PagingIdentifier::getSegmentName));

    for (PagingIdentifier previousPagingIdentifier : this.pagingIdentifiers) {
      if (!newPagingIdentifierNames.containsKey(previousPagingIdentifier.getSegmentName())) {
        newPagingIdentifiers.add(
          new PagingIdentifier(
            previousPagingIdentifier.getSegmentName(),
            previousPagingIdentifier.getSegmentOffset() + 1)
        );
      }
    }
    this.pagingIdentifiers = newPagingIdentifiers;
  }

  @Override
  public void close() throws Exception {
    if (writer != null) {
      writer.close();
    }
    if (pagingIdentifiers != null) {
      pagingIdentifiers.clear();
    }
    jsonReader = null;
  }
}
