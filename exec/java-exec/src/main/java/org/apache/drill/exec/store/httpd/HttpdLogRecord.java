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

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import io.netty.buffer.DrillBuf;
import java.util.EnumSet;
import java.util.Map;
import nl.basjes.parse.core.Casts;
import nl.basjes.parse.core.Parser;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.drill.exec.vector.complex.writer.BigIntWriter;
import org.apache.drill.exec.vector.complex.writer.Float8Writer;
import org.apache.drill.exec.vector.complex.writer.VarCharWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpdLogRecord {

  private static final Logger LOG = LoggerFactory.getLogger(HttpdLogRecord.class);
  private final Map<String, VarCharWriter> strings = Maps.newHashMap();
  private final Map<String, BigIntWriter> longs = Maps.newHashMap();
  private final Map<String, Float8Writer> doubles = Maps.newHashMap();
  private final Map<String, MapWriter> wildcards = Maps.newHashMap();
  private final Map<String, String> cleanExtensions = Maps.newHashMap();
  private final Map<String, MapWriter> startedWildcards = Maps.newHashMap();
  private final Map<String, MapWriter> wildcardWriters = Maps.newHashMap();
  private DrillBuf managedBuffer;

  public HttpdLogRecord(final DrillBuf managedBuffer) {
    this.managedBuffer = managedBuffer;
  }

  /**
   * Call this method after a record has been parsed. This finished the lifecycle of any maps that were written and
   * removes all the entries for the next record to be able to work.
   */
  public void finishRecord() {
    for (MapWriter writer : wildcardWriters.values()) {
      writer.end();
    }
    wildcardWriters.clear();
    startedWildcards.clear();
  }

  private DrillBuf buf(final int size) {
    if (managedBuffer.capacity() < size) {
      managedBuffer = managedBuffer.reallocIfNeeded(size);
    }
    return managedBuffer;
  }

  private void writeString(final VarCharWriter writer, final String value) {
    final byte[] stringBytes = value.getBytes(Charsets.UTF_8);
    final DrillBuf stringBuffer = buf(stringBytes.length);
    stringBuffer.clear();
    stringBuffer.writeBytes(stringBytes);
    writer.writeVarChar(0, stringBytes.length, stringBuffer);
  }

  /**
   * This method is referenced and called via reflection. This is added as a parsing target for the parser. It will get
   * called when the value of a log field is a String data type.
   *
   * @param field name of field
   * @param value value of field
   */
  @SuppressWarnings("unused")
  public void set(final String field, final String value) {
    if (value != null) {
      final VarCharWriter w = strings.get(field);
      if (w != null) {
        LOG.trace("Parsed field: {}, as string: {}", field, value);
        writeString(w, value);
      }
      else {
        LOG.warn("No 'string' writer found for field: {}", field);
      }
    }
  }

  /**
   * This method is referenced and called via reflection. This is added as a parsing target for the parser. It will get
   * called when the value of a log field is a Long data type.
   *
   * @param field name of field
   * @param value value of field
   */
  @SuppressWarnings("unused")
  public void set(final String field, final Long value) {
    if (value != null) {
      final BigIntWriter w = longs.get(field);
      if (w != null) {
        LOG.trace("Parsed field: {}, as long: {}", field, value);
        w.writeBigInt(value);
      }
      else {
        LOG.warn("No 'long' writer found for field: {}", field);
      }
    }
  }

  /**
   * This method is referenced and called via reflection. This is added as a parsing target for the parser. It will get
   * called when the value of a log field is a Double data type.
   *
   * @param field name of field
   * @param value value of field
   */
  @SuppressWarnings("unused")
  public void set(final String field, final Double value) {
    if (value != null) {
      final Float8Writer w = doubles.get(field);
      if (w != null) {
        LOG.trace("Parsed field: {}, as double: {}", field, value);
        w.writeFloat8(value);
      }
      else {
        LOG.warn("No 'double' writer found for field: {}", field);
      }
    }
  }

  /**
   * This method is referenced and called via reflection. When the parser processes a field like:
   * HTTP.URI:request.firstline.uri.query.* where star is an arbitrary field that the parser found this method will be
   * invoked. <br>
   *
   * @param field name of field
   * @param value value of field
   */
  @SuppressWarnings("unused")
  public void setWildcard(final String field, final String value) {
    if (value != null) {
      final MapWriter mapWriter = getWildcardWriter(field);
      LOG.trace("Parsed wildcard field: {}, as string: {}", field, value);
      final VarCharWriter w = mapWriter.varChar(cleanExtensions.get(field));
      writeString(w, value);
    }
  }

  /**
   * This method is referenced and called via reflection. When the parser processes a field like:
   * HTTP.URI:request.firstline.uri.query.* where star is an arbitrary field that the parser found this method will be
   * invoked. <br>
   *
   * @param field name of field
   * @param value value of field
   */
  @SuppressWarnings("unused")
  public void setWildcard(final String field, final Long value) {
    if (value != null) {
      final MapWriter mapWriter = getWildcardWriter(field);
      LOG.trace("Parsed wildcard field: {}, as long: {}", field, value);
      final BigIntWriter w = mapWriter.bigInt(cleanExtensions.get(field));
      w.writeBigInt(value);
    }
  }

  /**
   * This method is referenced and called via reflection. When the parser processes a field like:
   * HTTP.URI:request.firstline.uri.query.* where star is an arbitrary field that the parser found this method will be
   * invoked. <br>
   *
   * @param field name of field
   * @param value value of field
   */
  @SuppressWarnings("unused")
  public void setWildcard(final String field, final Double value) {
    if (value != null) {
      final MapWriter mapWriter = getWildcardWriter(field);
      LOG.trace("Parsed wildcard field: {}, as double: {}", field, value);
      final Float8Writer w = mapWriter.float8(cleanExtensions.get(field));
      w.writeFloat8(value);
    }
  }

  /**
   * For a configuration like HTTP.URI:request.firstline.uri.query.*, a writer was created with name
   * HTTP.URI:request.firstline.uri.query, we traverse the list of wildcard writers to see which one is the root of the
   * name of the field passed in like HTTP.URI:request.firstline.uri.query.old. This is writer entry that is needed.
   *
   * @param field like HTTP.URI:request.firstline.uri.query.old where 'old' is one of many different parameter names.
   * @return the writer to be used for this field.
   */
  private MapWriter getWildcardWriter(final String field) {
    MapWriter writer = startedWildcards.get(field);
    if (writer == null) {
      for (Map.Entry<String, MapWriter> entry : wildcards.entrySet()) {
        final String root = entry.getKey();
        if (field.startsWith(root)) {
          writer = entry.getValue();

          /**
           * In order to save some time, store the cleaned version of the field extension. It is possible it will have
           * unsafe characters in it.
           */
          if (!cleanExtensions.containsKey(field)) {
            final String extension = field.substring(root.length() + 1, field.length());
            final String cleanExtension = HttpdParser.drillFormattedFieldName(extension);
            cleanExtensions.put(field, cleanExtension);
            LOG.debug("Added extension: field='{}' with cleanExtension='{}'", field, cleanExtension);
          }

          /**
           * We already know we have the writer, but if we have put this writer in the started list, do NOT call start
           * again.
           */
          if (!wildcardWriters.containsKey(root)) {
            /**
             * Start and store this root map writer for later retrieval.
             */
            LOG.debug("Starting new wildcard field writer: {}", field);
            writer.start();
            startedWildcards.put(field, writer);
            wildcardWriters.put(root, writer);
          }

          /**
           * Break out of the for loop when we find a root writer that matches the field.
           */
          break;
        }
      }
    }

    return writer;
  }

  public Map<String, VarCharWriter> getStrings() {
    return strings;
  }

  public Map<String, BigIntWriter> getLongs() {
    return longs;
  }

  public Map<String, Float8Writer> getDoubles() {
    return doubles;
  }

  /**
   * This record will be used with a single parser. For each field that is to be parsed a setter will be called. It
   * registers a setter method for each field being parsed. It also builds the data writers to hold the data beings
   * parsed.
   *
   * @param parser
   * @param mapWriter
   * @param type
   * @param parserFieldName
   * @param drillFieldName
   * @throws NoSuchMethodException
   */
  public void addField(final Parser<HttpdLogRecord> parser, final MapWriter mapWriter, final EnumSet<Casts> type, final String parserFieldName, final String drillFieldName) throws NoSuchMethodException {
    final boolean hasWildcard = parserFieldName.endsWith(HttpdParser.PARSER_WILDCARD);

    /**
     * This is a dynamic way to map the setter for each specified field type. <br/>
     * e.g. a TIME.STAMP may map to a LONG while a referrer may map to a STRING
     */
    if (hasWildcard) {
      final String cleanName = parserFieldName.substring(0, parserFieldName.length() - HttpdParser.PARSER_WILDCARD.length());
      LOG.debug("Adding WILDCARD parse target: {} as {}, with field name: {}", parserFieldName, cleanName, drillFieldName);
      parser.addParseTarget(this.getClass().getMethod("setWildcard", String.class, String.class), parserFieldName);
      parser.addParseTarget(this.getClass().getMethod("setWildcard", String.class, Double.class), parserFieldName);
      parser.addParseTarget(this.getClass().getMethod("setWildcard", String.class, Long.class), parserFieldName);
      wildcards.put(cleanName, mapWriter.map(drillFieldName));
    }
    else if (type.contains(Casts.DOUBLE)) {
      LOG.debug("Adding DOUBLE parse target: {}, with field name: {}", parserFieldName, drillFieldName);
      parser.addParseTarget(this.getClass().getMethod("set", String.class, Double.class), parserFieldName);
      doubles.put(parserFieldName, mapWriter.float8(drillFieldName));
    }
    else if (type.contains(Casts.LONG)) {
      LOG.debug("Adding LONG parse target: {}, with field name: {}", parserFieldName, drillFieldName);
      parser.addParseTarget(this.getClass().getMethod("set", String.class, Long.class), parserFieldName);
      longs.put(parserFieldName, mapWriter.bigInt(drillFieldName));
    }
    else {
      LOG.debug("Adding STRING parse target: {}, with field name: {}", parserFieldName, drillFieldName);
      parser.addParseTarget(this.getClass().getMethod("set", String.class, String.class), parserFieldName);
      strings.put(parserFieldName, mapWriter.varChar(drillFieldName));
    }
  }
}