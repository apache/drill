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

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import io.netty.buffer.DrillBuf;
import nl.basjes.parse.core.Casts;
import nl.basjes.parse.core.Parser;
import nl.basjes.parse.core.exceptions.DissectionFailure;
import nl.basjes.parse.core.exceptions.InvalidDissectorException;
import nl.basjes.parse.core.exceptions.MissingDissectorsException;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HttpdParser {

  private static final Logger LOG = LoggerFactory.getLogger(HttpdParser.class);
  public static final String PARSER_WILDCARD = ".*";
  public static final String SAFE_WILDCARD = "_$";
  public static final String SAFE_SEPARATOR = "_";
  public static final String REMAPPING_FLAG = "#";
  private final Parser<HttpdLogRecord> parser;
  private final HttpdLogRecord record;

    public static final HashMap<String, String> LOGFIELDS = new HashMap<String, String>();
    static
    {
        LOGFIELDS.put("request_receive_time_weekyear__utc", "TIME_YEAR:request_receive_time_weekyear__utc");
        LOGFIELDS.put("request_referer_ref", "HTTP_REF:request_referer_ref");
        LOGFIELDS.put("request_referer_protocol", "HTTP_PROTOCOL:request_referer_protocol");
        LOGFIELDS.put("request_receive_time_timezone", "TIME_ZONE:request_receive_time_timezone");
        LOGFIELDS.put("connection_client_host", "IP:connection_client_host");
        LOGFIELDS.put("connection_client_ip", "IP:connection_client_ip");
        LOGFIELDS.put("connection_client_peerip", "IP:connection_client_peerip");
        LOGFIELDS.put("connection_server_ip", "IP:connection_server_ip");
        LOGFIELDS.put("request_receive_time_day", "TIME_DAY:request_receive_time_day");
        LOGFIELDS.put("request_receive_time_minute__utc", "TIME_MINUTE:request_receive_time_minute__utc");
        LOGFIELDS.put("request_referer_query_$", "STRING:request_referer_query_$");
        LOGFIELDS.put("request_receive_time_millisecond__utc", "TIME_MILLISECOND:request_receive_time_millisecond__utc");
        LOGFIELDS.put("request_firstline_uri_port", "HTTP_PORT:request_firstline_uri_port");
        LOGFIELDS.put("request_referer_userinfo", "HTTP_USERINFO:request_referer_userinfo");
        LOGFIELDS.put("request_receive_time_second__utc", "TIME_SECOND:request_receive_time_second__utc");
        LOGFIELDS.put("request_firstline_uri_protocol", "HTTP_PROTOCOL:request_firstline_uri_protocol");
        LOGFIELDS.put("request_receive_time_month", "TIME_MONTH:request_receive_time_month");
        LOGFIELDS.put("request_firstline_uri_query", "HTTP_QUERYSTRING:request_firstline_uri_query");
        LOGFIELDS.put("request_firstline_uri_path", "HTTP_PATH:request_firstline_uri_path");
        LOGFIELDS.put("request_receive_time_hour__utc", "TIME_HOUR:request_receive_time_hour__utc");
        LOGFIELDS.put("request_receive_time_monthname", "TIME_MONTHNAME:request_receive_time_monthname");
        LOGFIELDS.put("request_receive_time_year__utc", "TIME_YEAR:request_receive_time_year__utc");
        LOGFIELDS.put("request_receive_time_second", "TIME_SECOND:request_receive_time_second");
        LOGFIELDS.put("request_referer", "HTTP_URI:request_referer");
        LOGFIELDS.put("request_receive_time_monthname__utc", "TIME_MONTHNAME:request_receive_time_monthname__utc");
        LOGFIELDS.put("request_referer_path", "HTTP_PATH:request_referer_path");
        LOGFIELDS.put("request_receive_time_weekyear", "TIME_YEAR:request_receive_time_weekyear");
        LOGFIELDS.put("request_firstline_protocol", "HTTP_PROTOCOL:request_firstline_protocol");
        LOGFIELDS.put("request_referer_port", "HTTP_PORT:request_referer_port");
        LOGFIELDS.put("request_receive_time_minute", "TIME_MINUTE:request_receive_time_minute");
        LOGFIELDS.put("request_status_last", "STRING:request_status_last");
        LOGFIELDS.put("request_receive_time_hour", "TIME_HOUR:request_receive_time_hour");
        LOGFIELDS.put("request_firstline_protocol_version", "HTTP_PROTOCOL_VERSION:request_firstline_protocol_version");
        LOGFIELDS.put("request_receive_time", "TIME_STAMP:request_receive_time");
        LOGFIELDS.put("request_firstline_method", "HTTP_METHOD:request_firstline_method");
        LOGFIELDS.put("request_receive_time_epoch", "TIME_EPOCH:request_receive_time_epoch");
        LOGFIELDS.put("request_receive_time_weekofweekyear", "TIME_WEEK:request_receive_time_weekofweekyear");
        LOGFIELDS.put("request_firstline_uri_host", "HTTP_HOST:request_firstline_uri_host");
        LOGFIELDS.put("request_referer_query", "HTTP_QUERYSTRING:request_referer_query");
        LOGFIELDS.put("request_firstline_uri_userinfo", "HTTP_USERINFO:request_firstline_uri_userinfo");
        LOGFIELDS.put("response_body_bytes", "BYTES:response_body_bytes");
        LOGFIELDS.put("response_body_bytesclf", "BYTES:response_body_bytesclf");
        LOGFIELDS.put("request_referer_host", "HTTP_HOST:request_referer_host");
        LOGFIELDS.put("request_receive_time_weekofweekyear__utc", "TIME_WEEK:request_receive_time_weekofweekyear__utc");
        LOGFIELDS.put("request_firstline_uri", "HTTP_URI:request_firstline_uri");
        LOGFIELDS.put("request_firstline_uri_ref", "HTTP_REF:request_firstline_uri_ref");
        LOGFIELDS.put("request_receive_time_year", "TIME_YEAR:request_receive_time_year");
        LOGFIELDS.put("request_firstline", "HTTP_FIRSTLINE:request_firstline");
        LOGFIELDS.put("request_user-agent", "HTTP_USERAGENT:request_user-agent");
        LOGFIELDS.put("request_cookies", "HTTP_COOKIE:request_cookies");
        LOGFIELDS.put("server_process_time", "MICROSECONDS:server_process_time");
        LOGFIELDS.put("request_cookies_$", "HTTP_COOKIE:request_cookies_$");
        LOGFIELDS.put("server_environment_$", "VARIABLE:server_environment_$");
        LOGFIELDS.put("server_filename", "FILENAME:server_filename");
        LOGFIELDS.put("request_protocol", "PROTOCOL:request_protocol");
        LOGFIELDS.put("request_header_", "HTTP_HEADER:request_header_");
        LOGFIELDS.put("connection_keepalivecount", "NUMBER:connection_keepalivecount");
        LOGFIELDS.put("connection_client_logname", "NUMBER:connection_client_logname");
        LOGFIELDS.put("request_errorlogid", "STRING:request_errorlogid");
        LOGFIELDS.put("request_method", "HTTP_METHOD:request_method");
        LOGFIELDS.put("server_module_note_$", "STRING:server_module_note_$");
        LOGFIELDS.put("response_header_$", "HTTP_HEADER:response_header_$");
        LOGFIELDS.put("request_server_port_canonical", "PORT:request_server_port_canonical");
        LOGFIELDS.put("connection_server_port_canonical", "PORT:connection_server_port_canonical");
        LOGFIELDS.put("connection_server_port", "PORT:connection_server_port");
        LOGFIELDS.put("connection_client_port", "PORT:connection_client_port");
        LOGFIELDS.put("connection_server_child_processid", "NUMBER:connection_server_child_processid");
        LOGFIELDS.put("connection_server_child_threadid", "NUMBER:connection_server_child_threadid");
        LOGFIELDS.put("connection_server_child_hexthreadid", "NUMBER:connection_server_child_hexthreadid");
        LOGFIELDS.put("request_querystring", "HTTP_QUERYSTRING:request_querystring");
        LOGFIELDS.put("request_handler", "STRING:request_handler");
        LOGFIELDS.put("request_status_original", "STRING:request_status_original");
        LOGFIELDS.put("request_status_last", "STRING:request_status_last");
        LOGFIELDS.put("request_receive_time_begin_msec", "TIME_EPOCH:request_receive_time_begin_msec");
        LOGFIELDS.put("request_receive_time_end_msec", "TIME_EPOCH:request_receive_time_end_msec");
        LOGFIELDS.put("request_receive_time_begin_usec", "TIME_EPOCH_USEC:request_receive_time_begin_usec");
        LOGFIELDS.put("request_receive_time_begin_usec", "TIME_EPOCH_USEC:request_receive_time_begin_usec");
        LOGFIELDS.put("request_receive_time_end_usec", "TIME_EPOCH_USEC:request_receive_time_end_usec");
        LOGFIELDS.put("request_receive_time_begin_msec_frac", "TIME_EPOCH:request_receive_time_begin_msec_frac");
        LOGFIELDS.put("request_receive_time_begin_msec_frac", "TIME_EPOCH:request_receive_time_begin_msec_frac");
        LOGFIELDS.put("request_receive_time_end_msec_frac", "TIME_EPOCH:request_receive_time_end_msec_frac");
        LOGFIELDS.put("request_receive_time_begin_usec_frac", "TIME_EPOCH_USEC_FRAC:request_receive_time_begin_usec_frac");
        LOGFIELDS.put("request_receive_time_begin_usec_frac", "TIME_EPOCH_USEC_FRAC:request.receive.time.begin.usec_frac");
        LOGFIELDS.put("request_receive_time_end_usec_frac", "TIME_EPOCH_USEC_FRAC:request_receive_time_end_usec_frac");
        LOGFIELDS.put("response_server_processing_time", "SECONDS:response_server_processing_time");
        LOGFIELDS.put("connection_client_user", "STRING:connection_client_user");
        LOGFIELDS.put("request_urlpath", "URI:request_urlpath");
        LOGFIELDS.put("connection_server_name_canonical", "STRING:connection_server_name_canonical");
        LOGFIELDS.put("connection_server_name", "STRING:connection_server_name");
        LOGFIELDS.put("response_connection_status", "HTTP_CONNECTSTATUS:response_connection_status");
        LOGFIELDS.put("request_bytes", "BYTES:request_bytes");
        LOGFIELDS.put("response_bytes", "BYTES:response_bytes");
    }

    //Map map = Collections.synchronizedMap(LOGFIELDS);

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
      String tempFieldName;
      tempFieldName = LOGFIELDS.get(drillFieldName);
      return tempFieldName.replace(SAFE_WILDCARD, PARSER_WILDCARD).replaceAll(SAFE_SEPARATOR, ".").replaceAll("\\.\\.", "_");

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

      if (parserFieldName.contains(":") ) {
        String[] fieldPart= parserFieldName.split(":");
        return fieldPart[1].replaceAll("_", "__").replace(PARSER_WILDCARD, SAFE_WILDCARD).replaceAll("\\.", SAFE_SEPARATOR);
        }
    else{
      return parserFieldName.replaceAll("_", "__").replace(PARSER_WILDCARD, SAFE_WILDCARD).replaceAll("\\.", SAFE_SEPARATOR);
    }
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