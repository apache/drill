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

  private static final Logger logger = LoggerFactory.getLogger(HttpdParser.class);

  public static final String PARSER_WILDCARD = ".*";
  public static final String SAFE_WILDCARD = "_$";
  public static final String SAFE_SEPARATOR = "_";
  public static final String REMAPPING_FLAG = "#";
  private final Parser<HttpdLogRecord> parser;
  private final HttpdLogRecord record;

  public static final HashMap<String, String> LOGFIELDS = new HashMap<String, String>();

  static {
    LOGFIELDS.put("connection.client.ip", "IP:connection.client.ip");
    LOGFIELDS.put("connection.client.ip.last", "IP:connection.client.ip.last");
    LOGFIELDS.put("connection.client.ip.original", "IP:connection.client.ip.original");
    LOGFIELDS.put("connection.client.ip.last", "IP:connection.client.ip.last");
    LOGFIELDS.put("connection.client.peerip", "IP:connection.client.peerip");
    LOGFIELDS.put("connection.client.peerip.last", "IP:connection.client.peerip.last");
    LOGFIELDS.put("connection.client.peerip.original", "IP:connection.client.peerip.original");
    LOGFIELDS.put("connection.client.peerip.last", "IP:connection.client.peerip.last");
    LOGFIELDS.put("connection.server.ip", "IP:connection.server.ip");
    LOGFIELDS.put("connection.server.ip.last", "IP:connection.server.ip.last");
    LOGFIELDS.put("connection.server.ip.original", "IP:connection.server.ip.original");
    LOGFIELDS.put("connection.server.ip.last", "IP:connection.server.ip.last");
    LOGFIELDS.put("response.body.bytes", "BYTES:response.body.bytes");
    LOGFIELDS.put("response.body.bytes.last", "BYTES:response.body.bytes.last");
    LOGFIELDS.put("response.body.bytes.original", "BYTES:response.body.bytes.original");
    LOGFIELDS.put("response.body.bytes.last", "BYTES:response.body.bytes.last");
    LOGFIELDS.put("response.body.bytesclf", "BYTES:response.body.bytesclf");
    LOGFIELDS.put("response.body.bytes", "BYTESCLF:response.body.bytes");
    LOGFIELDS.put("response.body.bytes.last", "BYTESCLF:response.body.bytes.last");
    LOGFIELDS.put("response.body.bytes.original", "BYTESCLF:response.body.bytes.original");
    LOGFIELDS.put("response.body.bytes.last", "BYTESCLF:response.body.bytes.last");
    LOGFIELDS.put("request.cookies.foobar", "HTTP.COOKIE:request.cookies.foobar");
    LOGFIELDS.put("server.environment.foobar", "VARIABLE:server.environment.foobar");
    LOGFIELDS.put("server.filename", "FILENAME:server.filename");
    LOGFIELDS.put("server.filename.last", "FILENAME:server.filename.last");
    LOGFIELDS.put("server.filename.original", "FILENAME:server.filename.original");
    LOGFIELDS.put("server.filename.last", "FILENAME:server.filename.last");
    LOGFIELDS.put("connection.client.host", "IP:connection.client.host");
    LOGFIELDS.put("connection.client.host.last", "IP:connection.client.host.last");
    LOGFIELDS.put("connection.client.host.original", "IP:connection.client.host.original");
    LOGFIELDS.put("connection.client.host.last", "IP:connection.client.host.last");
    LOGFIELDS.put("request.protocol", "PROTOCOL:request.protocol");
    LOGFIELDS.put("request.protocol.last", "PROTOCOL:request.protocol.last");
    LOGFIELDS.put("request.protocol.original", "PROTOCOL:request.protocol.original");
    LOGFIELDS.put("request.protocol.last", "PROTOCOL:request.protocol.last");
    LOGFIELDS.put("request.header.foobar", "HTTP.HEADER:request.header.foobar");
    LOGFIELDS.put("request.trailer.foobar", "HTTP.TRAILER:request.trailer.foobar");
    LOGFIELDS.put("connection.keepalivecount", "NUMBER:connection.keepalivecount");
    LOGFIELDS.put("connection.keepalivecount.last", "NUMBER:connection.keepalivecount.last");
    LOGFIELDS.put("connection.keepalivecount.original", "NUMBER:connection.keepalivecount.original");
    LOGFIELDS.put("connection.keepalivecount.last", "NUMBER:connection.keepalivecount.last");
    LOGFIELDS.put("connection.client.logname", "NUMBER:connection.client.logname");
    LOGFIELDS.put("connection.client.logname.last", "NUMBER:connection.client.logname.last");
    LOGFIELDS.put("connection.client.logname.original", "NUMBER:connection.client.logname.original");
    LOGFIELDS.put("connection.client.logname.last", "NUMBER:connection.client.logname.last");
    LOGFIELDS.put("request.errorlogid", "STRING:request.errorlogid");
    LOGFIELDS.put("request.errorlogid.last", "STRING:request.errorlogid.last");
    LOGFIELDS.put("request.errorlogid.original", "STRING:request.errorlogid.original");
    LOGFIELDS.put("request.errorlogid.last", "STRING:request.errorlogid.last");
    LOGFIELDS.put("request.method", "HTTP.METHOD:request.method");
    LOGFIELDS.put("request.method.last", "HTTP.METHOD:request.method.last");
    LOGFIELDS.put("request.method.original", "HTTP.METHOD:request.method.original");
    LOGFIELDS.put("request.method.last", "HTTP.METHOD:request.method.last");
    LOGFIELDS.put("server.module_note.foobar", "STRING:server.module_note.foobar");
    LOGFIELDS.put("response.header.foobar", "HTTP.HEADER:response.header.foobar");
    LOGFIELDS.put("response.trailer.foobar", "HTTP.TRAILER:response.trailer.foobar");
    LOGFIELDS.put("request.server.port.canonical", "PORT:request.server.port.canonical");
    LOGFIELDS.put("request.server.port.canonical.last", "PORT:request.server.port.canonical.last");
    LOGFIELDS.put("request.server.port.canonical.original", "PORT:request.server.port.canonical.original");
    LOGFIELDS.put("request.server.port.canonical.last", "PORT:request.server.port.canonical.last");
    LOGFIELDS.put("connection.server.port.canonical", "PORT:connection.server.port.canonical");
    LOGFIELDS.put("connection.server.port.canonical.last", "PORT:connection.server.port.canonical.last");
    LOGFIELDS.put("connection.server.port.canonical.original", "PORT:connection.server.port.canonical.original");
    LOGFIELDS.put("connection.server.port.canonical.last", "PORT:connection.server.port.canonical.last");
    LOGFIELDS.put("connection.server.port", "PORT:connection.server.port");
    LOGFIELDS.put("connection.server.port.last", "PORT:connection.server.port.last");
    LOGFIELDS.put("connection.server.port.original", "PORT:connection.server.port.original");
    LOGFIELDS.put("connection.server.port.last", "PORT:connection.server.port.last");
    LOGFIELDS.put("connection.client.port", "PORT:connection.client.port");
    LOGFIELDS.put("connection.client.port.last", "PORT:connection.client.port.last");
    LOGFIELDS.put("connection.client.port.original", "PORT:connection.client.port.original");
    LOGFIELDS.put("connection.client.port.last", "PORT:connection.client.port.last");
    LOGFIELDS.put("connection.server.child.processid", "NUMBER:connection.server.child.processid");
    LOGFIELDS.put("connection.server.child.processid.last", "NUMBER:connection.server.child.processid.last");
    LOGFIELDS.put("connection.server.child.processid.original", "NUMBER:connection.server.child.processid.original");
    LOGFIELDS.put("connection.server.child.processid.last", "NUMBER:connection.server.child.processid.last");
    LOGFIELDS.put("connection.server.child.processid", "NUMBER:connection.server.child.processid");
    LOGFIELDS.put("connection.server.child.processid.last", "NUMBER:connection.server.child.processid.last");
    LOGFIELDS.put("connection.server.child.processid.original", "NUMBER:connection.server.child.processid.original");
    LOGFIELDS.put("connection.server.child.processid.last", "NUMBER:connection.server.child.processid.last");
    LOGFIELDS.put("connection.server.child.threadid", "NUMBER:connection.server.child.threadid");
    LOGFIELDS.put("connection.server.child.threadid.last", "NUMBER:connection.server.child.threadid.last");
    LOGFIELDS.put("connection.server.child.threadid.original", "NUMBER:connection.server.child.threadid.original");
    LOGFIELDS.put("connection.server.child.threadid.last", "NUMBER:connection.server.child.threadid.last");
    LOGFIELDS.put("connection.server.child.hexthreadid", "NUMBER:connection.server.child.hexthreadid");
    LOGFIELDS.put("connection.server.child.hexthreadid.last", "NUMBER:connection.server.child.hexthreadid.last");
    LOGFIELDS.put("connection.server.child.hexthreadid.original", "NUMBER:connection.server.child.hexthreadid.original");
    LOGFIELDS.put("connection.server.child.hexthreadid.last", "NUMBER:connection.server.child.hexthreadid.last");
    LOGFIELDS.put("request.querystring", "HTTP.QUERYSTRING:request.querystring");
    LOGFIELDS.put("request.querystring.last", "HTTP.QUERYSTRING:request.querystring.last");
    LOGFIELDS.put("request.querystring.original", "HTTP.QUERYSTRING:request.querystring.original");
    LOGFIELDS.put("request.querystring.last", "HTTP.QUERYSTRING:request.querystring.last");
    LOGFIELDS.put("request.firstline", "HTTP.FIRSTLINE:request.firstline");
    LOGFIELDS.put("request.firstline.original", "HTTP.FIRSTLINE:request.firstline.original");
    LOGFIELDS.put("request.firstline.original", "HTTP.FIRSTLINE:request.firstline.original");
    LOGFIELDS.put("request.firstline.last", "HTTP.FIRSTLINE:request.firstline.last");
    LOGFIELDS.put("request.handler", "STRING:request.handler");
    LOGFIELDS.put("request.handler.last", "STRING:request.handler.last");
    LOGFIELDS.put("request.handler.original", "STRING:request.handler.original");
    LOGFIELDS.put("request.handler.last", "STRING:request.handler.last");
    LOGFIELDS.put("request.status", "STRING:request.status");
    LOGFIELDS.put("request.status.original", "STRING:request.status.original");
    LOGFIELDS.put("request.status.original", "STRING:request.status.original");
    LOGFIELDS.put("request.status.last", "STRING:request.status.last");
    LOGFIELDS.put("request.receive.time", "TIME.STAMP:request.receive.time");
    LOGFIELDS.put("request.receive.time.last", "TIME.STAMP:request.receive.time.last");
    LOGFIELDS.put("request.receive.time.original", "TIME.STAMP:request.receive.time.original");
    LOGFIELDS.put("request.receive.time.last", "TIME.STAMP:request.receive.time.last");
    LOGFIELDS.put("request.receive.time.year", "TIME.YEAR:request.receive.time.year");
    LOGFIELDS.put("request.receive.time.begin.year", "TIME.YEAR:request.receive.time.begin.year");
    LOGFIELDS.put("request.receive.time.end.year", "TIME.YEAR:request.receive.time.end.year");
    LOGFIELDS.put("request.receive.time.sec", "TIME.SECONDS:request.receive.time.sec");
    LOGFIELDS.put("request.receive.time.sec", "TIME.SECONDS:request.receive.time.sec");
    LOGFIELDS.put("request.receive.time.sec.original", "TIME.SECONDS:request.receive.time.sec.original");
    LOGFIELDS.put("request.receive.time.sec.last", "TIME.SECONDS:request.receive.time.sec.last");
    LOGFIELDS.put("request.receive.time.begin.sec", "TIME.SECONDS:request.receive.time.begin.sec");
    LOGFIELDS.put("request.receive.time.begin.sec.last", "TIME.SECONDS:request.receive.time.begin.sec.last");
    LOGFIELDS.put("request.receive.time.begin.sec.original", "TIME.SECONDS:request.receive.time.begin.sec.original");
    LOGFIELDS.put("request.receive.time.begin.sec.last", "TIME.SECONDS:request.receive.time.begin.sec.last");
    LOGFIELDS.put("request.receive.time.end.sec", "TIME.SECONDS:request.receive.time.end.sec");
    LOGFIELDS.put("request.receive.time.end.sec.last", "TIME.SECONDS:request.receive.time.end.sec.last");
    LOGFIELDS.put("request.receive.time.end.sec.original", "TIME.SECONDS:request.receive.time.end.sec.original");
    LOGFIELDS.put("request.receive.time.end.sec.last", "TIME.SECONDS:request.receive.time.end.sec.last");
    LOGFIELDS.put("request.receive.time.begin.msec", "TIME.EPOCH:request.receive.time.begin.msec");
    LOGFIELDS.put("request.receive.time.msec", "TIME.EPOCH:request.receive.time.msec");
    LOGFIELDS.put("request.receive.time.msec.last", "TIME.EPOCH:request.receive.time.msec.last");
    LOGFIELDS.put("request.receive.time.msec.original", "TIME.EPOCH:request.receive.time.msec.original");
    LOGFIELDS.put("request.receive.time.msec.last", "TIME.EPOCH:request.receive.time.msec.last");
    LOGFIELDS.put("request.receive.time.begin.msec", "TIME.EPOCH:request.receive.time.begin.msec");
    LOGFIELDS.put("request.receive.time.begin.msec.last", "TIME.EPOCH:request.receive.time.begin.msec.last");
    LOGFIELDS.put("request.receive.time.begin.msec.original", "TIME.EPOCH:request.receive.time.begin.msec.original");
    LOGFIELDS.put("request.receive.time.begin.msec.last", "TIME.EPOCH:request.receive.time.begin.msec.last");
    LOGFIELDS.put("request.receive.time.end.msec", "TIME.EPOCH:request.receive.time.end.msec");
    LOGFIELDS.put("request.receive.time.end.msec.last", "TIME.EPOCH:request.receive.time.end.msec.last");
    LOGFIELDS.put("request.receive.time.end.msec.original", "TIME.EPOCH:request.receive.time.end.msec.original");
    LOGFIELDS.put("request.receive.time.end.msec.last", "TIME.EPOCH:request.receive.time.end.msec.last");
    LOGFIELDS.put("request.receive.time.begin.usec", "TIME.EPOCH.USEC:request.receive.time.begin.usec");
    LOGFIELDS.put("request.receive.time.usec", "TIME.EPOCH.USEC:request.receive.time.usec");
    LOGFIELDS.put("request.receive.time.usec.last", "TIME.EPOCH.USEC:request.receive.time.usec.last");
    LOGFIELDS.put("request.receive.time.usec.original", "TIME.EPOCH.USEC:request.receive.time.usec.original");
    LOGFIELDS.put("request.receive.time.usec.last", "TIME.EPOCH.USEC:request.receive.time.usec.last");
    LOGFIELDS.put("request.receive.time.begin.usec", "TIME.EPOCH.USEC:request.receive.time.begin.usec");
    LOGFIELDS.put("request.receive.time.begin.usec.last", "TIME.EPOCH.USEC:request.receive.time.begin.usec.last");
    LOGFIELDS.put("request.receive.time.begin.usec.original", "TIME.EPOCH.USEC:request.receive.time.begin.usec.original");
    LOGFIELDS.put("request.receive.time.begin.usec.last", "TIME.EPOCH.USEC:request.receive.time.begin.usec.last");
    LOGFIELDS.put("request.receive.time.end.usec", "TIME.EPOCH.USEC:request.receive.time.end.usec");
    LOGFIELDS.put("request.receive.time.end.usec.last", "TIME.EPOCH.USEC:request.receive.time.end.usec.last");
    LOGFIELDS.put("request.receive.time.end.usec.original", "TIME.EPOCH.USEC:request.receive.time.end.usec.original");
    LOGFIELDS.put("request.receive.time.end.usec.last", "TIME.EPOCH.USEC:request.receive.time.end.usec.last");
    LOGFIELDS.put("request.receive.time.begin.msec_frac", "TIME.EPOCH:request.receive.time.begin.msec_frac");
    LOGFIELDS.put("request.receive.time.msec_frac", "TIME.EPOCH:request.receive.time.msec_frac");
    LOGFIELDS.put("request.receive.time.msec_frac.last", "TIME.EPOCH:request.receive.time.msec_frac.last");
    LOGFIELDS.put("request.receive.time.msec_frac.original", "TIME.EPOCH:request.receive.time.msec_frac.original");
    LOGFIELDS.put("request.receive.time.msec_frac.last", "TIME.EPOCH:request.receive.time.msec_frac.last");
    LOGFIELDS.put("request.receive.time.begin.msec_frac", "TIME.EPOCH:request.receive.time.begin.msec_frac");
    LOGFIELDS.put("request.receive.time.begin.msec_frac.last", "TIME.EPOCH:request.receive.time.begin.msec_frac.last");
    LOGFIELDS.put("request.receive.time.begin.msec_frac.original", "TIME.EPOCH:request.receive.time.begin.msec_frac.original");
    LOGFIELDS.put("request.receive.time.begin.msec_frac.last", "TIME.EPOCH:request.receive.time.begin.msec_frac.last");
    LOGFIELDS.put("request.receive.time.end.msec_frac", "TIME.EPOCH:request.receive.time.end.msec_frac");
    LOGFIELDS.put("request.receive.time.end.msec_frac.last", "TIME.EPOCH:request.receive.time.end.msec_frac.last");
    LOGFIELDS.put("request.receive.time.end.msec_frac.original", "TIME.EPOCH:request.receive.time.end.msec_frac.original");
    LOGFIELDS.put("request.receive.time.end.msec_frac.last", "TIME.EPOCH:request.receive.time.end.msec_frac.last");
    LOGFIELDS.put("request.receive.time.begin.usec_frac", "FRAC:request.receive.time.begin.usec_frac");
    LOGFIELDS.put("request.receive.time.usec_frac", "FRAC:request.receive.time.usec_frac");
    LOGFIELDS.put("request.receive.time.usec_frac.last", "FRAC:request.receive.time.usec_frac.last");
    LOGFIELDS.put("request.receive.time.usec_frac.original", "FRAC:request.receive.time.usec_frac.original");
    LOGFIELDS.put("request.receive.time.usec_frac.last", "FRAC:request.receive.time.usec_frac.last");
    LOGFIELDS.put("request.receive.time.begin.usec_frac", "FRAC:request.receive.time.begin.usec_frac");
    LOGFIELDS.put("request.receive.time.begin.usec_frac.last", "FRAC:request.receive.time.begin.usec_frac.last");
    LOGFIELDS.put("request.receive.time.begin.usec_frac.original", "FRAC:request.receive.time.begin.usec_frac.original");
    LOGFIELDS.put("request.receive.time.begin.usec_frac.last", "FRAC:request.receive.time.begin.usec_frac.last");
    LOGFIELDS.put("request.receive.time.end.usec_frac", "FRAC:request.receive.time.end.usec_frac");
    LOGFIELDS.put("request.receive.time.end.usec_frac.last", "FRAC:request.receive.time.end.usec_frac.last");
    LOGFIELDS.put("request.receive.time.end.usec_frac.original", "FRAC:request.receive.time.end.usec_frac.original");
    LOGFIELDS.put("request.receive.time.end.usec_frac.last", "FRAC:request.receive.time.end.usec_frac.last");
    LOGFIELDS.put("response.server.processing.time", "SECONDS:response.server.processing.time");
    LOGFIELDS.put("response.server.processing.time.original", "SECONDS:response.server.processing.time.original");
    LOGFIELDS.put("response.server.processing.time.original", "SECONDS:response.server.processing.time.original");
    LOGFIELDS.put("response.server.processing.time.last", "SECONDS:response.server.processing.time.last");
    LOGFIELDS.put("server.process.time", "MICROSECONDS:server.process.time");
    LOGFIELDS.put("response.server.processing.time", "MICROSECONDS:response.server.processing.time");
    LOGFIELDS.put("response.server.processing.time.original", "MICROSECONDS:response.server.processing.time.original");
    LOGFIELDS.put("response.server.processing.time.original", "MICROSECONDS:response.server.processing.time.original");
    LOGFIELDS.put("response.server.processing.time.last", "MICROSECONDS:response.server.processing.time.last");
    LOGFIELDS.put("response.server.processing.time", "MICROSECONDS:response.server.processing.time");
    LOGFIELDS.put("response.server.processing.time.original", "MICROSECONDS:response.server.processing.time.original");
    LOGFIELDS.put("response.server.processing.time.original", "MICROSECONDS:response.server.processing.time.original");
    LOGFIELDS.put("response.server.processing.time.last", "MICROSECONDS:response.server.processing.time.last");
    LOGFIELDS.put("response.server.processing.time", "MILLISECONDS:response.server.processing.time");
    LOGFIELDS.put("response.server.processing.time.original", "MILLISECONDS:response.server.processing.time.original");
    LOGFIELDS.put("response.server.processing.time.original", "MILLISECONDS:response.server.processing.time.original");
    LOGFIELDS.put("response.server.processing.time.last", "MILLISECONDS:response.server.processing.time.last");
    LOGFIELDS.put("response.server.processing.time", "SECONDS:response.server.processing.time");
    LOGFIELDS.put("response.server.processing.time.original", "SECONDS:response.server.processing.time.original");
    LOGFIELDS.put("response.server.processing.time.original", "SECONDS:response.server.processing.time.original");
    LOGFIELDS.put("response.server.processing.time.last", "SECONDS:response.server.processing.time.last");
    LOGFIELDS.put("connection.client.user", "STRING:connection.client.user");
    LOGFIELDS.put("connection.client.user.last", "STRING:connection.client.user.last");
    LOGFIELDS.put("connection.client.user.original", "STRING:connection.client.user.original");
    LOGFIELDS.put("connection.client.user.last", "STRING:connection.client.user.last");
    LOGFIELDS.put("request.urlpath", "URI:request.urlpath");
    LOGFIELDS.put("request.urlpath.original", "URI:request.urlpath.original");
    LOGFIELDS.put("request.urlpath.original", "URI:request.urlpath.original");
    LOGFIELDS.put("request.urlpath.last", "URI:request.urlpath.last");
    LOGFIELDS.put("connection.server.name.canonical", "STRING:connection.server.name.canonical");
    LOGFIELDS.put("connection.server.name.canonical.last", "STRING:connection.server.name.canonical.last");
    LOGFIELDS.put("connection.server.name.canonical.original", "STRING:connection.server.name.canonical.original");
    LOGFIELDS.put("connection.server.name.canonical.last", "STRING:connection.server.name.canonical.last");
    LOGFIELDS.put("connection.server.name", "STRING:connection.server.name");
    LOGFIELDS.put("connection.server.name.last", "STRING:connection.server.name.last");
    LOGFIELDS.put("connection.server.name.original", "STRING:connection.server.name.original");
    LOGFIELDS.put("connection.server.name.last", "STRING:connection.server.name.last");
    LOGFIELDS.put("response.connection.status", "HTTP.CONNECTSTATUS:response.connection.status");
    LOGFIELDS.put("response.connection.status.last", "HTTP.CONNECTSTATUS:response.connection.status.last");
    LOGFIELDS.put("response.connection.status.original", "HTTP.CONNECTSTATUS:response.connection.status.original");
    LOGFIELDS.put("response.connection.status.last", "HTTP.CONNECTSTATUS:response.connection.status.last");
    LOGFIELDS.put("request.bytes", "BYTES:request.bytes");
    LOGFIELDS.put("request.bytes.last", "BYTES:request.bytes.last");
    LOGFIELDS.put("request.bytes.original", "BYTES:request.bytes.original");
    LOGFIELDS.put("request.bytes.last", "BYTES:request.bytes.last");
    LOGFIELDS.put("response.bytes", "BYTES:response.bytes");
    LOGFIELDS.put("response.bytes.last", "BYTES:response.bytes.last");
    LOGFIELDS.put("response.bytes.original", "BYTES:response.bytes.original");
    LOGFIELDS.put("response.bytes.last", "BYTES:response.bytes.last");
    LOGFIELDS.put("total.bytes", "BYTES:total.bytes");
    LOGFIELDS.put("total.bytes.last", "BYTES:total.bytes.last");
    LOGFIELDS.put("total.bytes.original", "BYTES:total.bytes.original");
    LOGFIELDS.put("total.bytes.last", "BYTES:total.bytes.last");
    LOGFIELDS.put("request.cookies", "HTTP.COOKIES:request.cookies");
    LOGFIELDS.put("request.cookies.last", "HTTP.COOKIES:request.cookies.last");
    LOGFIELDS.put("request.cookies.original", "HTTP.COOKIES:request.cookies.original");
    LOGFIELDS.put("request.cookies.last", "HTTP.COOKIES:request.cookies.last");
    LOGFIELDS.put("response.cookies", "HTTP.SETCOOKIES:response.cookies");
    LOGFIELDS.put("response.cookies.last", "HTTP.SETCOOKIES:response.cookies.last");
    LOGFIELDS.put("response.cookies.original", "HTTP.SETCOOKIES:response.cookies.original");
    LOGFIELDS.put("response.cookies.last", "HTTP.SETCOOKIES:response.cookies.last");
    LOGFIELDS.put("request.user-agent", "HTTP.USERAGENT:request.user-agent");
    LOGFIELDS.put("request.user-agent.last", "HTTP.USERAGENT:request.user-agent.last");
    LOGFIELDS.put("request.user-agent.original", "HTTP.USERAGENT:request.user-agent.original");
    LOGFIELDS.put("request.user-agent.last", "HTTP.USERAGENT:request.user-agent.last");
    LOGFIELDS.put("request.referer", "HTTP.URI:request.referer");
    LOGFIELDS.put("request.referer.last", "HTTP.URI:request.referer.last");
    LOGFIELDS.put("request.referer.original", "HTTP.URI:request.referer.original");
    LOGFIELDS.put("request.referer.last", "HTTP.URI:request.referer.last");
  }

  public HttpdParser(final MapWriter mapWriter, final DrillBuf managedBuffer, final String logFormat,
                     final String timestampFormat, final Map<String, String> fieldMapping)
          throws NoSuchMethodException, MissingDissectorsException, InvalidDissectorException {

    Preconditions.checkArgument(logFormat != null && !logFormat.trim().isEmpty(), "logFormat cannot be null or empty");

    this.record = new HttpdLogRecord(managedBuffer, timestampFormat);
    this.parser = new HttpdLoglineParser<>(HttpdLogRecord.class, logFormat, timestampFormat);

    setupParser(mapWriter, logFormat, fieldMapping);

    if (timestampFormat != null && !timestampFormat.trim().isEmpty()) {
      logger.info("Custom timestamp format has been specified. This is an informational note only as custom timestamps is rather unusual.");
    }
    if (logFormat.contains("\n")) {
      logger.info("Specified logformat is a multiline log format: {}", logFormat);
    }
  }

  /**
   * We do not expose the underlying parser or the record which is used to manage the writers.
   *
   * @param line log line to tear apart.
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
   * @param parser    Add type remapping to this parser instance.
   * @param fieldName request.firstline.uri.query.[parameter_name]
   * @param fieldType HTTP.URI, etc..
   */
  private void addTypeRemapping(final Parser<HttpdLogRecord> parser, final String fieldName, final String fieldType) {
    logger.debug("Adding type remapping - fieldName: {}, fieldType: {}", fieldName, fieldType);
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
  public static String parserFormattedFieldName(String drillFieldName) {

    //The Useragent fields contain a dash which causes potential problems if the field name is not escaped properly
    //This removes the dash
    if (drillFieldName.contains("useragent")) {
      drillFieldName = drillFieldName.replace("useragent", "user-agent");
    }

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
  public static String drillFormattedFieldName(String parserFieldName) {

    //The Useragent fields contain a dash which causes potential problems if the field name is not escaped properly
    //This removes the dash
    if (parserFieldName.contains("user-agent")) {
      parserFieldName = parserFieldName.replace("user-agent", "useragent");
    }

    if (parserFieldName.contains(":")) {
      String[] fieldPart = parserFieldName.split(":");
      return fieldPart[1].replaceAll("_", "__").replace(PARSER_WILDCARD, SAFE_WILDCARD).replaceAll("\\.", SAFE_SEPARATOR);
    } else {
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
      logger.debug("Using fields defined by user.");
      requestedPaths = fieldMapping;
    } else {
      /**
       * Use all possible paths that the parser has determined from the specified log format.
       */
      logger.debug("No fields defined by user, defaulting to all possible fields.");
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
      } else {
        casts = dummy.getCasts(entry.getValue());
      }

      logger.debug("Setting up drill field: {}, parser field: {}, which casts as: {}", entry.getKey(), entry.getValue(), casts);
      record.addField(parser, mapWriter, casts, entry.getValue(), entry.getKey());
    }
  }
}
