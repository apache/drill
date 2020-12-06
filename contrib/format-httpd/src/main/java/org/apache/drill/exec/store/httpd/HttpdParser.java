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
package org.apache.drill.exec.store.httpd;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import nl.basjes.parse.core.Casts;
import nl.basjes.parse.core.Parser;
import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpdParser {

  private static final Logger logger = LoggerFactory.getLogger(HttpdParser.class);

  public static final String PARSER_WILDCARD = ".*";
  public static final String REMAPPING_FLAG = "#";
  private final Parser<HttpdLogRecord> parser;
  private final List<SchemaPath> requestedColumns;
  private final Map<String, MinorType> mappedColumns;
  private final HttpdLogRecord record;
  private final String logFormat;
  private Map<String, String> requestedPaths;
  private EnumSet<Casts> casts;


  public HttpdParser(final String logFormat, final String timestampFormat, final boolean flattenWildcards, final EasySubScan scan) {

    Preconditions.checkArgument(logFormat != null && !logFormat.trim().isEmpty(), "logFormat cannot be null or empty");

    this.logFormat = logFormat;
    this.record = new HttpdLogRecord(timestampFormat, flattenWildcards);

    if (timestampFormat == null) {
      this.parser = new HttpdLoglineParser<>(HttpdLogRecord.class, logFormat);
    } else {
      this.parser = new HttpdLoglineParser<>(HttpdLogRecord.class, logFormat, timestampFormat);
    }

    /*
    * The log parser has the possibility of parsing the user agent and extracting additional fields
    * Unfortunately, doing so negatively affects the speed of the parser.  Uncommenting this line and another in
    * the HttpLogRecord will enable these fields.  We will add this functionality in a future PR.
    * this.parser.addDissector(new UserAgentDissector());
    */

    this.requestedColumns = scan.getColumns();

    if (timestampFormat != null && !timestampFormat.trim().isEmpty()) {
      logger.info("Custom timestamp format has been specified. This is an informational note only as custom timestamps is rather unusual.");
    }
    if (logFormat.contains("\n")) {
      logger.info("Specified logformat is a multiline log format: {}", logFormat);
    }

    mappedColumns = new HashMap<>();
  }

  /**
   * We do not expose the underlying parser or the record which is used to manage the writers.
   *
   * @param line log line to tear apart.
   * @throws DissectionFailure if there is a generic dissector failure
   * @throws InvalidDissectorException if the dissector is not valid
   * @throws MissingDissectorsException if the dissector is missing
   */
  public void parse(final String line) throws DissectionFailure, InvalidDissectorException, MissingDissectorsException {
    parser.parse(record, line);
    record.finishRecord();
  }

  public TupleMetadata setupParser()
          throws NoSuchMethodException, MissingDissectorsException, InvalidDissectorException {

    SchemaBuilder builder = new SchemaBuilder();

    /*
     * If the user has selected fields, then we will use them to configure the parser because this would be the most
     * efficient way to parse the log.
     */
    List<String> allParserPaths = parser.getPossiblePaths();

    /*
     * Use all possible paths that the parser has determined from the specified log format.
     */

    requestedPaths = Maps.newConcurrentMap();

    for (final String parserPath : allParserPaths) {
      requestedPaths.put(HttpdUtils.drillFormattedFieldName(parserPath), parserPath);
    }

    /*
     * By adding the parse target to the dummy instance we activate it for use. Which we can then use to find out which
     * paths cast to which native data types. After we are done figuring this information out, we throw this away
     * because this will be the slowest parsing path possible for the specified format.
     */
    Parser<Object> dummy = new HttpdLoglineParser<>(Object.class, logFormat);

    /* This is the second line to uncomment to add the user agent parsing.
    * dummy.addDissector(new UserAgentDissector());
    */
    dummy.addParseTarget(String.class.getMethod("indexOf", String.class), allParserPaths);

    for (final Map.Entry<String, String> entry : requestedPaths.entrySet()) {

      /*
      If the column is not requested explicitly, remove it from the requested path list.
       */
      if (! isRequested(entry.getKey()) &&
        !(isStarQuery()) &&
        !isMetadataQuery() &&
        !isOnlyImplicitColumns() ) {
        requestedPaths.remove(entry.getKey());
        continue;
      }

      /*
       * Check the field specified by the user to see if it is supposed to be remapped.
       */
      if (entry.getValue().startsWith(REMAPPING_FLAG)) {
        /*
         * Because this field is being remapped we need to replace the field name that the parser uses.
         */
        entry.setValue(entry.getValue().substring(REMAPPING_FLAG.length()));

        final String[] pieces = entry.getValue().split(":");
        HttpdUtils.addTypeRemapping(parser, pieces[1], pieces[0]);
        casts = Casts.STRING_ONLY;
      } else {
        casts = dummy.getCasts(entry.getValue());
      }

      Casts dataType = (Casts) casts.toArray()[casts.size() - 1];

      switch (dataType) {
        case STRING:
          if (entry.getValue().startsWith("TIME.STAMP:")) {
            builder.addNullable(entry.getKey(), MinorType.TIMESTAMP);
            mappedColumns.put(entry.getKey(), MinorType.TIMESTAMP);
          } else if (entry.getValue().startsWith("TIME.DATE:")) {
            builder.addNullable(entry.getKey(), MinorType.DATE);
            mappedColumns.put(entry.getKey(), MinorType.DATE);
          } else if (entry.getValue().startsWith("TIME.TIME:")) {
            builder.addNullable(entry.getKey(), MinorType.TIME);
            mappedColumns.put(entry.getKey(), MinorType.TIME);
          } else if (HttpdUtils.isWildcard(entry.getValue())) {
            builder.addMap(entry.getValue());
            mappedColumns.put(entry.getKey(), MinorType.MAP);
          }
          else {
            builder.addNullable(entry.getKey(), TypeProtos.MinorType.VARCHAR);
            mappedColumns.put(entry.getKey(), MinorType.VARCHAR);
          }
          break;
        case LONG:
          if (entry.getValue().startsWith("TIME.EPOCH:")) {
            builder.addNullable(entry.getKey(), MinorType.TIMESTAMP);
            mappedColumns.put(entry.getKey(), MinorType.TIMESTAMP);
          } else {
            builder.addNullable(entry.getKey(), TypeProtos.MinorType.BIGINT);
            mappedColumns.put(entry.getKey(), MinorType.BIGINT);
          }
          break;
        case DOUBLE:
          builder.addNullable(entry.getKey(), TypeProtos.MinorType.FLOAT8);
          mappedColumns.put(entry.getKey(), MinorType.FLOAT8);
          break;
        default:
          logger.error("HTTPD Unsupported data type {} for field {}", dataType.toString(), entry.getKey());
          break;
      }
    }
    return builder.build();
  }

  public void addFieldsToParser(RowSetLoader rowWriter) {
    for (final Map.Entry<String, String> entry : requestedPaths.entrySet()) {
      try {
        record.addField(parser, rowWriter, casts, entry.getValue(), entry.getKey(), mappedColumns);
      } catch (NoSuchMethodException e) {
        logger.error("Error adding fields to parser.");
      }
    }
    logger.debug("Added Fields to Parser");
  }

  public boolean isStarQuery() {
    return requestedColumns.size() == 1 && requestedColumns.get(0).isDynamicStar();
  }

  public boolean isMetadataQuery() {
    return requestedColumns.size() == 0;
  }

  public boolean isRequested(String colName) {
    for (SchemaPath path : requestedColumns) {
      if (path.isDynamicStar()) {
        return true;
      } else if (path.nameEquals(colName)) {
        return true;
      }
    }
    return false;
  }

  /*
  This is for the edge case where a query only contains the implicit fields.
   */
  public boolean isOnlyImplicitColumns() {

    // If there are more than two columns, this isn't an issue.
    if (requestedColumns.size() > 2) {
      return false;
    }

    if (requestedColumns.size() == 1) {
      return requestedColumns.get(0).nameEquals(HttpdLogBatchReader.RAW_LINE_COL_NAME) ||
        requestedColumns.get(0).nameEquals(HttpdLogBatchReader.MATCHED_COL_NAME);
    } else {
      return (requestedColumns.get(0).nameEquals(HttpdLogBatchReader.RAW_LINE_COL_NAME) ||
        requestedColumns.get(0).nameEquals(HttpdLogBatchReader.MATCHED_COL_NAME)) &&
        (requestedColumns.get(1).nameEquals(HttpdLogBatchReader.RAW_LINE_COL_NAME) ||
        requestedColumns.get(1).nameEquals(HttpdLogBatchReader.MATCHED_COL_NAME));
    }
  }
}
