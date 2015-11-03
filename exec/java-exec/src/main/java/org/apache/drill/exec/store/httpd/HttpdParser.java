/*
 * Copyright 2015 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.httpd;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.netty.buffer.DrillBuf;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import nl.basjes.parse.core.Casts;
import nl.basjes.parse.core.Parser;
import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpdParser {

  private static final Logger LOG = LoggerFactory.getLogger(HttpdParser.class);
  public static final String PARSER_WILDCARD = ".*";
  public static final String SAFE_WILDCARD = "_$";
  public static final String SAFE_SEPARATOR = "_";
  public static final String REMAPPING_FLAG = "#";
  private final Parser<HttpdLogRecord> parser;
  private final HttpdLogRecord record;

  public HttpdParser(final MapWriter mapWriter, final DrillBuf managedBuffer, final String logFormat,
      final String timestampFormat, final Map<String, String> fieldMapping)
      throws NoSuchMethodException, MissingDissectorsException, InvalidDissectorException {

    Preconditions.checkArgument(logFormat != null && !logFormat.trim().isEmpty(), "logFormat cannot be null or empty");

    this.record = new HttpdLogRecord(managedBuffer);
    this.parser = new HttpdLoglineParser<>(HttpdLogRecord.class, logFormat, timestampFormat);

    setupParser(mapWriter, logFormat, fieldMapping);

    if (timestampFormat != null && !timestampFormat.trim().isEmpty()) {
      LOG.info("Custom timestamp format has been specified. This is an informational note only as custom timestamps is rather unusual.");
    }
    if (logFormat.contains("\n")) {
      LOG.info("Specified logformat is a multiline log format: {}", logFormat);
    }
  }

  /**
   * We do not expose the underlying parser or the record which is used to manage the writers.
   *
   * @param line log line to tear apart.
   *
   * @throws DissectionFailure
   * @throws InvalidDissectorException
   * @throws MissingDissectorsException
   */
  public void parse(final String line) throws DissectionFailure, InvalidDissectorException, MissingDissectorsException {
    parser.parse(record, line);
    record.finishRecord();
  }

  /**
   * In order to define a type remapping the format of the field configuration will look like: <br/>
   * HTTP.URI:request.firstline.uri.query.[parameter name] <br/>
   *
   * @param parser Add type remapping to this parser instance.
   * @param fieldName request.firstline.uri.query.[parameter_name]
   * @param fieldType HTTP.URI, etc..
   */
  private void addTypeRemapping(final Parser<HttpdLogRecord> parser, final String fieldName, final String fieldType) {
    LOG.debug("Adding type remapping - fieldName: {}, fieldType: {}", fieldName, fieldType);
    parser.addTypeRemapping(fieldName, fieldType);
  }

  /**
   * The parser deals with dots unlike Drill wanting underscores request_referer. For the sake of simplicity we are
   * going replace the dots. The resultant output field will look like: request.referer.<br>
   * Additionally, wild cards will get replaced with .*
   *
   * @param drillFieldName name to be cleansed.
   * @return
   */
  public static String parserFormattedFieldName(final String drillFieldName) {
    return drillFieldName.replace(SAFE_WILDCARD, PARSER_WILDCARD).replaceAll(SAFE_SEPARATOR, ".").replaceAll("\\.\\.", "_");
  }

  /**
   * Drill cannot deal with fields with dots in them like request.referer. For the sake of simplicity we are going
   * ensure the field name is cleansed. The resultant output field will look like: request_referer.<br>
   * Additionally, wild cards will get replaced with _$
   *
   * @param parserFieldName name to be cleansed.
   * @return
   */
  public static String drillFormattedFieldName(final String parserFieldName) {
    return parserFieldName.replaceAll("_", "__").replace(PARSER_WILDCARD, SAFE_WILDCARD).replaceAll("\\.", SAFE_SEPARATOR);
  }

  private void setupParser(final MapWriter mapWriter, final String logFormat, final Map<String, String> fieldMapping)
      throws NoSuchMethodException, MissingDissectorsException, InvalidDissectorException {

    /**
     * If the user has selected fields, then we will use them to configure the parser because this would be the most
     * efficient way to parse the log.
     */
    final Map<String, String> requestedPaths;
    final List<String> allParserPaths = parser.getPossiblePaths();
    if (fieldMapping != null && !fieldMapping.isEmpty()) {
      LOG.debug("Using fields defined by user.");
      requestedPaths = fieldMapping;
    }
    else {
      /**
       * Use all possible paths that the parser has determined from the specified log format.
       */
      LOG.debug("No fields defined by user, defaulting to all possible fields.");
      requestedPaths = Maps.newHashMap();
      for (final String parserPath : allParserPaths) {
        requestedPaths.put(drillFormattedFieldName(parserPath), parserPath);
      }
    }

    /**
     * By adding the parse target to the dummy instance we activate it for use. Which we can then use to find out which
     * paths cast to which native data types. After we are done figuring this information out, we throw this away
     * because this will be the slowest parsing path possible for the specified format.
     */
    Parser<Object> dummy = new HttpdLoglineParser<>(Object.class, logFormat);
    dummy.addParseTarget(String.class.getMethod("indexOf", String.class), allParserPaths);

    for (final Map.Entry<String, String> entry : requestedPaths.entrySet()) {
      final EnumSet<Casts> casts;

      /**
       * Check the field specified by the user to see if it is supposed to be remapped.
       */
      if (entry.getValue().startsWith(REMAPPING_FLAG)) {
        /**
         * Because this field is being remapped we need to replace the field name that the parser uses.
         */
        entry.setValue(entry.getValue().substring(REMAPPING_FLAG.length()));

        final String[] pieces = entry.getValue().split(":");
        addTypeRemapping(parser, pieces[1], pieces[0]);

        casts = Casts.STRING_ONLY;
      }
      else {
        casts = dummy.getCasts(entry.getValue());
      }

      LOG.debug("Setting up drill field: {}, parser field: {}, which casts as: {}", entry.getKey(), entry.getValue(), casts);
      record.addField(parser, mapWriter, casts, entry.getValue(), entry.getKey());
    }
  }
}
