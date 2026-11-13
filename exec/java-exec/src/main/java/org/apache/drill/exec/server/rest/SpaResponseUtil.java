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
package org.apache.drill.exec.server.rest;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URL;

/**
 * Stream the React SPA's {@code index.html} as a JAX-RS Response. Used by
 * resources that need to run a server-side side effect (e.g. updating
 * session state before login) but still want the SPA to render the page
 * body, mirroring what {@link SpaServletFilter} does for paths it owns.
 */
final class SpaResponseUtil {
  private static final Logger logger = LoggerFactory.getLogger(SpaResponseUtil.class);
  private static final String INDEX_DIST = "/webapp/dist/index.html";
  private static final String INDEX_SOURCE = "/webapp/index.html";

  private SpaResponseUtil() {
  }

  /**
   * @return 200 OK with the SPA index.html as the body, or 404 if the webapp
   *         hasn't been built into the classpath.
   */
  static Response serveSpaIndex() {
    URL resource = SpaResponseUtil.class.getResource(INDEX_DIST);
    if (resource == null) {
      // Dev fallback for runs where `npm run build` hasn't been executed.
      resource = SpaResponseUtil.class.getResource(INDEX_SOURCE);
    }
    if (resource == null) {
      logger.warn("SPA index.html not found on classpath; was the webapp built?");
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    final URL src = resource;
    StreamingOutput body = output -> {
      try (InputStream in = src.openStream()) {
        in.transferTo(output);
      }
    };
    return Response.ok(body).type("text/html; charset=UTF-8").build();
  }
}
