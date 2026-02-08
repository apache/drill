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

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Servlet for serving the SQL Lab React SPA.
 * Handles client-side routing by serving index.html for all non-static routes.
 */
public class SqlLabSpaServlet extends HttpServlet {
  private static final Logger logger = LoggerFactory.getLogger(SqlLabSpaServlet.class);
  private static final String INDEX_HTML = "index.html";
  private static final Path WEBAPP_BASE = Paths.get("webapp");

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String pathInfo = request.getPathInfo();
    if (pathInfo == null || pathInfo.equals("/")) {
      pathInfo = "/" + INDEX_HTML;
    }

    // Check if this is a static asset request (has file extension)
    boolean isStaticAsset = pathInfo.lastIndexOf('.') > pathInfo.lastIndexOf('/');

    // Try to serve from built assets first
    String subPath = isStaticAsset ? "dist" + pathInfo : "dist/" + INDEX_HTML;
    URL resource = resolveResource(subPath);

    // Fallback to development mode (source files)
    if (resource == null && isStaticAsset) {
      resource = resolveResource(pathInfo.substring(1));
    }

    // For SPA routing, always serve index.html for non-static requests
    if (resource == null && !isStaticAsset) {
      resource = getClass().getResource("/webapp/dist/" + INDEX_HTML);

      // Fallback to source index.html
      if (resource == null) {
        resource = getClass().getResource("/webapp/" + INDEX_HTML);
      }
    }

    if (resource == null) {
      logger.debug("Resource not found: {}", pathInfo);
      response.sendError(HttpServletResponse.SC_NOT_FOUND);
      return;
    }

    // Set content type based on file extension
    String contentType = getContentType(pathInfo);
    if (contentType != null) {
      response.setContentType(contentType);
    }

    // Serve the resource
    try (InputStream in = resource.openStream();
         OutputStream out = response.getOutputStream()) {
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = in.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
      }
    }
  }

  /**
   * Resolve a classpath resource within the webapp directory, using
   * java.nio.file.Path normalization to prevent path traversal.
   *
   * @param subPath the relative path within webapp (e.g. "dist/assets/index.js")
   * @return the resource URL, or null if invalid or not found
   */
  private URL resolveResource(String subPath) {
    try {
      Path resolved = WEBAPP_BASE.resolve(subPath).normalize();
      if (!resolved.startsWith(WEBAPP_BASE)) {
        logger.debug("Blocked path traversal attempt: {}", subPath);
        return null;
      }
      String resourcePath = "/" + resolved.toString().replace('\\', '/');
      return getClass().getResource(resourcePath);
    } catch (InvalidPathException e) {
      return null;
    }
  }

  private String getContentType(String path) {
    if (path.endsWith(".html")) {
      return "text/html; charset=UTF-8";
    } else if (path.endsWith(".js")) {
      return "application/javascript; charset=UTF-8";
    } else if (path.endsWith(".css")) {
      return "text/css; charset=UTF-8";
    } else if (path.endsWith(".json")) {
      return "application/json; charset=UTF-8";
    } else if (path.endsWith(".png")) {
      return "image/png";
    } else if (path.endsWith(".jpg") || path.endsWith(".jpeg")) {
      return "image/jpeg";
    } else if (path.endsWith(".svg")) {
      return "image/svg+xml";
    } else if (path.endsWith(".ico")) {
      return "image/x-icon";
    } else if (path.endsWith(".woff")) {
      return "font/woff";
    } else if (path.endsWith(".woff2")) {
      return "font/woff2";
    } else if (path.endsWith(".ttf")) {
      return "font/ttf";
    } else if (path.endsWith(".eot")) {
      return "application/vnd.ms-fontobject";
    }
    return null;
  }
}
