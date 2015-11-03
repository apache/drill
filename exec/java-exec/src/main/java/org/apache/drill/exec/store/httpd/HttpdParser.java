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
import io.netty.buffer.DrillBuf;
import java.util.EnumSet;
import java.util.List;
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
  private final Parser<HttpdLogRecord> parser;
  private final HttpdLogRecord record;

  public HttpdParser(final MapWriter mapWriter, final DrillBuf managedBuffer, final String logFormat,
      final String timestampFormat, final List<String> configuredFields)
      throws NoSuchMethodException, MissingDissectorsException, InvalidDissectorException {

    Preconditions.checkArgument(logFormat != null && !logFormat.trim().isEmpty(), "logFormat cannot be null or empty");

    this.record = new HttpdLogRecord(managedBuffer);
    this.parser = new HttpdLoglineParser<>(HttpdLogRecord.class, logFormat, timestampFormat);

    setupParser(mapWriter, logFormat, configuredFields);

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
   * STRING:HTTP.URI:request.firstline.uri.query.[parameter name] <br/>
   *
   * STRING, DOUBLE, LONG are the only valid options for the Cast type.
   *
   * @param parser Add type remapping to this parser instance.
   * @param fieldName request.firstline.uri.query.[parameter_name]
   * @param fieldType HTTP.URI, etc..
   * @param castType STRING, DOUBLE, or LONG
   */
  private void addTypeRemapping(final Parser<HttpdLogRecord> parser, final String fieldName, final String fieldType, final String castType) {
    final EnumSet<Casts> casts = EnumSet.of(Casts.valueOf(castType.toUpperCase()));
    LOG.debug("Adding type remapping - fieldName: {}, fieldType: {}, castType: {}", fieldName, fieldType, casts);
    parser.addTypeRemapping(fieldName, fieldType, casts);
  }

  private void setupParser(final MapWriter mapWriter, final String logFormat, final List<String> configuredFields)
      throws NoSuchMethodException, MissingDissectorsException, InvalidDissectorException {

    /**
     * If the configuration has fields defined, then we will use them because this would be the most efficient way to
     * parse the log. Otherwise we will use all possible paths that the parser has determined from the log format
     * because the user doesn't know exactly what they want.
     */
    final List<String> possiblePaths;
    if (configuredFields != null && !configuredFields.isEmpty()) {
      LOG.debug("Using fields defined by user.");
      possiblePaths = configuredFields;
    }
    else {
      LOG.debug("No fields defined by user, defaulting to all possible fields.");
      possiblePaths = parser.getPossiblePaths();
    }

    /**
     * By adding the parse target to the dummy instance we activate it for use. Which we can then use to find out which
     * paths cast to which native data types. After we are done figuring this information out, we throw this away
     * because this will be the slowest parsing path possible for the specified format.
     */
    Parser<Object> dummy = new HttpdLoglineParser<>(Object.class, logFormat);
    dummy.addParseTarget(String.class.getMethod("indexOf", String.class), possiblePaths);

    for (final String path : possiblePaths) {
      final EnumSet<Casts> casts = dummy.getCasts(path);
      LOG.debug("Setting up path: {}, which casts: {}", path, casts);

      /**
       * Check the field specified by the user to see if it is supposed to be remapped. If it is supposed to be remapped
       * then remap and setup the field name for the setter without the cast type in the name.
       */
      final String field;
      final String[] pieces = path.split(":");
      if (pieces.length == 3) {
        addTypeRemapping(parser, pieces[2], pieces[1], pieces[0]);
        field = pieces[1] + ":" + pieces[2];
      }
      else {
        field = path;
      }

      record.addField(parser, mapWriter, casts, field);
    }
  }
}
