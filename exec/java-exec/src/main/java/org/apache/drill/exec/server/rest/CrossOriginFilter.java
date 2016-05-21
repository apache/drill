/**
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

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;


public class CrossOriginFilter implements Filter {
    // Response headers
    public static final String ACCESS_CONTROL_ALLOW_ORIGIN_HEADER = "Access-Control-Allow-Origin";
    // Implementation constants
    public static final String ALLOWED_ORIGINS_PARAM = "allowedOrigins";
    public static final String ALLOWED_METHODS_PARAM = "allowedMethods";
    public static final String ALLOWED_HEADERS_PARAM = "allowedHeaders";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        handle((HttpServletRequest) servletRequest, (HttpServletResponse) servletResponse, filterChain);
    }

    private void handle(HttpServletRequest servletRequest, HttpServletResponse servletResponse, FilterChain filterChain) {
        servletResponse.setHeader(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
        servletResponse.setHeader(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN_HEADER, "*");
        servletResponse.setHeader(CrossOriginFilter.ALLOWED_METHODS_PARAM, "GET,POST,HEAD,OPTIONS");
        servletResponse.setHeader(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "X-Requested-With,Content-Type,Accept,Origin");
    }

    @Override
    public void destroy() {

    }
}
