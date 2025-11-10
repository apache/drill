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
package org.apache.drill.exec.server.rest.auth;

import org.apache.drill.exec.server.rest.WebServerConstants;
import org.eclipse.jetty.ee10.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.util.Callback;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.Writer;

/**
 * Custom ErrorHandler class for Drill's WebServer to handle errors appropriately based on the request type.
 * - For JSON API endpoints (*.json), returns JSON error responses
 * - For SPNEGO login failures, provides helpful HTML error page
 * - For all other cases, returns standard HTML error page
 */
public class DrillErrorHandler extends ErrorPageErrorHandler {

  @Override
  public boolean handle(Request target, org.eclipse.jetty.server.Response response, Callback callback) throws Exception {
    // Check if this is a JSON API request
    String pathInContext = Request.getPathInContext(target);
    if (pathInContext != null && pathInContext.endsWith(".json")) {
      // For JSON API endpoints, return JSON error response instead of HTML
      response.getHeaders().put("Content-Type", "application/json");

      String jsonError = "{\n  \"errorMessage\" : \"Query submission failed\"\n}";

      // Write the JSON response
      response.write(true, java.nio.ByteBuffer.wrap(
          jsonError.getBytes(java.nio.charset.StandardCharsets.UTF_8)), callback);
      return true;
    }

    // For non-JSON requests, use default HTML error handling
    return super.handle(target, response, callback);
  }

  @Override
  protected void writeErrorPageMessage(HttpServletRequest request, Writer writer,
                                       int code, String message, String uri) throws IOException {

    super.writeErrorPageMessage(request, writer, code, message, uri);

    if (uri != null && uri.equals(WebServerConstants.SPENGO_LOGIN_RESOURCE_PATH)) {
      writer.write("<p>SPNEGO Login Failed</p>");
      writer.write("<p>Please check the requirements or use below link to use Form Authentication instead</p>");
      writer.write("<a href='/login'> login </a>");
    }
  }
}