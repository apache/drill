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

import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenericExceptionMapper implements ExceptionMapper<Throwable> {
  private static final Logger logger = LoggerFactory.getLogger(GenericExceptionMapper.class);

  @Override
  public Response toResponse(Throwable throwable) {
    // Don't intercept WebApplicationExceptions (including NotFoundException) - let Jersey handle them
    // These are normal HTTP responses, not internal errors
    if (throwable instanceof WebApplicationException) {
      WebApplicationException webAppException = (WebApplicationException) throwable;
      logger.debug("WebApplicationException: {} - returning status {}",
          throwable.getMessage(), webAppException.getResponse().getStatus());
      return webAppException.getResponse();
    }

    String errorMessage = throwable.getMessage();
    if (errorMessage == null) {
      errorMessage = throwable.getClass().getSimpleName();
    }

    logger.error("REST API error - returning 500 response", throwable);

    return Response
        .status(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())
        .entity(new GenericErrorMessage(errorMessage))
        .type(MediaType.APPLICATION_JSON_TYPE).build();
  }

  public static class GenericErrorMessage {
    public final String errorMessage;

    public GenericErrorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
    }
  }
}
