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

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

/**
 * Serves the React single-page app. Runs before the Jersey servlet:
 *
 * <ul>
 *   <li>Requests for known Jersey paths (the {@link #JERSEY_EXACT_PATHS} /
 *       {@link #JERSEY_PREFIXES} allow-list and anything ending in
 *       {@code .json}) chain through to Jersey untouched.</li>
 *   <li>Requests for paths with a file extension are served as static assets
 *       from {@code /webapp/dist/} on the classpath; if no such asset exists,
 *       a {@code 404} is returned (without falling through to Jersey, since
 *       Jersey would just 404 too and a flat 404 is more accurate).</li>
 *   <li>Everything else is treated as a React Router route and served the
 *       SPA's {@code index.html} so client-side routing can take over.</li>
 * </ul>
 *
 * <p>This filter exists because Jersey's JAX-RS class-path matching treats a
 * catch-all {@code @Path("/{path:.*}")} resource as MORE specific than the
 * existing root resources (it has more capturing groups), so a JAX-RS-based
 * catch-all would shadow every JSON endpoint at the root. Running before
 * Jersey, with an explicit allow-list, avoids the shadowing.</p>
 */
public class SpaServletFilter implements Filter {
  private static final Logger logger = LoggerFactory.getLogger(SpaServletFilter.class);
  private static final String INDEX_HTML = "index.html";
  private static final String DIST_BASE = "/webapp/dist/";
  private static final String SOURCE_BASE = "/webapp/";
  private static final Path WEBAPP_BASE = Paths.get("webapp");

  /**
   * Exact paths that always belong to a JAX-RS resource. Anything ending in
   * {@code .json} is also routed to Jersey via a {@code endsWith} check, so
   * those don't need to appear here.
   */
  private static final Set<String> JERSEY_EXACT_PATHS = Set.of(
      // DrillRoot (cluster info + shutdown)
      "/state", "/queriesCount", "/portNum", "/gracePeriod",
      "/gracefulShutdown", "/quiescent", "/shutdown",
      // Login / logout — Jersey handles GET /login and GET /mainLogin to
      // populate the session redirect target before serving the SPA;
      // /spnegoLogin and /logout return server-side redirects.
      "/login", "/spnegoLogin", "/mainLogin", "/logout",
      // Health-check HTML page consumed by external monitors / load balancers.
      "/status"
  );

  /**
   * Path prefixes that always belong to JAX-RS resources. Any request whose
   * URI starts with one of these passes through to Jersey.
   */
  private static final String[] JERSEY_PREFIXES = {
      "/api/",
      "/storage/",
      "/profiles/",
      "/credentials/",
      "/option/",
      "/logs/",
      "/status/",
      "/gracefulShutdown/",
      "/dynamic/",
      "/static/"
  };

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    if (!(request instanceof HttpServletRequest) || !(response instanceof HttpServletResponse)) {
      chain.doFilter(request, response);
      return;
    }
    HttpServletRequest httpReq = (HttpServletRequest) request;
    HttpServletResponse httpResp = (HttpServletResponse) response;

    String path = httpReq.getRequestURI();
    if (path == null || path.isEmpty()) {
      path = "/";
    }
    String method = httpReq.getMethod();

    // Anything other than GET / HEAD belongs to Jersey: POST/PUT/DELETE on a
    // SPA path would be a bug; let Jersey 404 it.
    if (!"GET".equalsIgnoreCase(method) && !"HEAD".equalsIgnoreCase(method)) {
      chain.doFilter(request, response);
      return;
    }

    if (isJerseyPath(path)) {
      chain.doFilter(request, response);
      return;
    }

    if (hasExtension(path)) {
      serveStaticAsset(path, httpResp);
      return;
    }

    serveIndexHtml(httpResp);
  }

  @Override
  public void destroy() {
  }

  private static boolean isJerseyPath(String path) {
    if (path.endsWith(".json")) {
      return true;
    }
    if (JERSEY_EXACT_PATHS.contains(path)) {
      return true;
    }
    for (String prefix : JERSEY_PREFIXES) {
      if (path.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasExtension(String path) {
    int lastDot = path.lastIndexOf('.');
    int lastSlash = path.lastIndexOf('/');
    return lastDot > lastSlash;
  }

  private void serveStaticAsset(String path, HttpServletResponse response) throws IOException {
    String relative = path.startsWith("/") ? path.substring(1) : path;
    URL resource = resolveAsset("dist/" + relative);
    if (resource == null) {
      // Dev fallback: source files live next to dist/, e.g. /vite.svg in public/.
      resource = resolveAsset(relative);
    }
    if (resource == null) {
      logger.debug("Static asset not found: {}", path);
      response.sendError(HttpServletResponse.SC_NOT_FOUND);
      return;
    }
    String contentType = contentTypeFor(path);
    if (contentType != null) {
      response.setContentType(contentType);
    }
    streamResource(resource, response);
  }

  private void serveIndexHtml(HttpServletResponse response) throws IOException {
    URL resource = getClass().getResource(DIST_BASE + INDEX_HTML);
    if (resource == null) {
      // Dev fallback for runs where the webapp hasn't been built.
      resource = getClass().getResource(SOURCE_BASE + INDEX_HTML);
    }
    if (resource == null) {
      logger.warn("SPA index.html not found on classpath; was the webapp built?");
      response.sendError(HttpServletResponse.SC_NOT_FOUND);
      return;
    }
    response.setContentType("text/html; charset=UTF-8");
    streamResource(resource, response);
  }

  private URL resolveAsset(String subPath) {
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

  private static void streamResource(URL resource, HttpServletResponse response) throws IOException {
    try (InputStream in = resource.openStream(); ServletOutputStream out = response.getOutputStream()) {
      in.transferTo(out);
    }
  }

  private static String contentTypeFor(String path) {
    if (path.endsWith(".html")) {
      return "text/html; charset=UTF-8";
    } else if (path.endsWith(".js") || path.endsWith(".mjs")) {
      return "application/javascript; charset=UTF-8";
    } else if (path.endsWith(".css")) {
      return "text/css; charset=UTF-8";
    } else if (path.endsWith(".map")) {
      return "application/json; charset=UTF-8";
    } else if (path.endsWith(".png")) {
      return "image/png";
    } else if (path.endsWith(".jpg") || path.endsWith(".jpeg")) {
      return "image/jpeg";
    } else if (path.endsWith(".gif")) {
      return "image/gif";
    } else if (path.endsWith(".svg")) {
      return "image/svg+xml";
    } else if (path.endsWith(".webp")) {
      return "image/webp";
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
    } else if (path.endsWith(".txt")) {
      return "text/plain; charset=UTF-8";
    } else if (path.endsWith(".wasm")) {
      return "application/wasm";
    }
    return null;
  }
}
