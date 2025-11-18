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
import org.eclipse.jetty.ee10.servlet.ServletContextRequest;
import org.eclipse.jetty.http.MimeTypes;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;

/**
 * Custom ErrorHandler class for Drill's WebServer to handle errors appropriately based on content negotiation.
 * <p>
 * This handler extends Jetty's ErrorPageErrorHandler to provide:
 * <ul>
 *   <li>JSON error responses when the client's Accept header indicates JSON is acceptable</li>
 *   <li>Custom HTML error pages for SPNEGO login failures with helpful guidance</li>
 *   <li>Standard HTML error pages for all other error conditions</li>
 * </ul>
 * <p>
 * Content negotiation is handled by Jetty's ErrorHandler framework, which evaluates the Accept header
 * and calls {@link #generateAcceptableResponse} with the appropriate content type.
 */
public class  DrillErrorHandler extends ErrorPageErrorHandler {

  /**
   * Generates an error response for the negotiated content type.
   * <p>
   * This method is called by Jetty's error handling framework after content negotiation has been performed
   * based on the client's Accept header. It provides custom formatting for JSON responses while delegating
   * to the parent class for HTML and other content types.
   *
   * @param baseRequest the base request object
   * @param request the HTTP servlet request
   * @param response the HTTP servlet response
   * @param code the HTTP error status code
   * @param message the error message to display
   * @param contentType the negotiated content type (e.g., "application/json", "text/html")
   * @throws IOException if an I/O error occurs while writing the response
   */
  @Override
  protected void generateAcceptableResponse(ServletContextRequest baseRequest,
                                           HttpServletRequest request,
                                           HttpServletResponse response,
                                           int code,
                                           String message,
                                           String contentType) throws IOException {
    // Handle JSON error responses when client accepts JSON
    if (contentType != null && (contentType.startsWith(MimeTypes.Type.APPLICATION_JSON.asString()) ||
                                contentType.startsWith(MimeTypes.Type.TEXT_JSON.asString()))) {
      response.setContentType(MimeTypes.Type.APPLICATION_JSON.asString());
      response.setCharacterEncoding(StandardCharsets.UTF_8.name());

      String jsonError = "{\n  \"errorMessage\" : \"" + message + "\"\n}";
      response.getWriter().write(jsonError);
      return;
    }

    // For all other content types (HTML, plain text, etc.), use default error handling
    super.generateAcceptableResponse(baseRequest, request, response, code, message, contentType);
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
