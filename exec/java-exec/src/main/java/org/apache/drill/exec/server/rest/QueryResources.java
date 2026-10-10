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

import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;
import org.apache.drill.exec.server.rest.stream.QueryRunner;
import org.apache.drill.exec.work.WorkManager;

import jakarta.annotation.security.RolesAllowed;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.StreamingOutput;

import java.io.IOException;
import java.io.OutputStream;

@Path("/")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class QueryResources {

  @Inject
  WorkManager work;

  @Inject
  WebUserConnection webUserConnection;

  @POST
  @Path("/query.json")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(externalDocs = @ExternalDocumentation(description = "Apache Drill REST API documentation:", url = "https://drill.apache.org/docs/rest-api-introduction/"))
  public StreamingOutput submitQueryJSON(QueryWrapper query) throws Exception {

    /*
    Prior to Drill 1.18, REST queries would batch the entire result set in memory,
    limiting query size. In Drill 1.19 and later, results are streamed from the
    executor, to the JSON writer and to the HTTP connection with no buffering.

    Starting with Drill 1.19, the "metadata" property specifying the result column
    data types is placed *before* the data itself. One drawback of doing so is that
    the schema will report that of the first batch: Drill allows schema to change
    across batches and thus the schema of the JSON-encoded data would change. This
    is more a bug with how Drill handles schemas than a JSON issue. (ODBC and JDBC
    have the same issues.)
    */
    QueryRunner runner = new QueryRunner(work, webUserConnection);
    try {
      runner.start(query);
    } catch (Exception e) {
      throw new WebApplicationException("Query submission failed", e);
    }
    return new StreamingOutput() {
      @Override
      public void write(OutputStream output)
        throws IOException, WebApplicationException {
        try {
          runner.sendResults(output);
        } catch (IOException e) {
          throw e;
        } catch (Exception e) {
          throw new WebApplicationException("JSON query failed", e);
        }
      }
    };
  }
}
