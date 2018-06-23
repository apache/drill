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

import io.netty.buffer.DrillBuf;
import java.util.Map;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpdParserTest {

  private static final Logger LOG = LoggerFactory.getLogger(HttpdParserTest.class);

  private void runTest(String logFormat, String logLine) throws Exception {
    MapWriter mapWriter = null;
    DrillBuf managedBuffer = null;
    Map<String, String> configuredFields = null;
    HttpdParser parser = new HttpdParser(mapWriter, managedBuffer, logFormat, null, configuredFields);
    parser.parse(logLine);
  }

//  @Test
  public void testFirstPattern() throws Exception {
    LOG.info("testFirstPattern");
//    final String format = "common";
//    final String format = "%h %l %u %t \"%r\" %>s %b";
    final String format = "%h %t \"%r\" %>s %b \"%{Referer}i\"";
    final String line = "127.0.0.1 [31/Dec/2012:23:49:41 +0100] "
        + "\"GET /foo HTTP/1.1\" 200 "
        + "1213 \"http://localhost/index.php?mies=wim\"";
    runTest(format, line);
  }

}