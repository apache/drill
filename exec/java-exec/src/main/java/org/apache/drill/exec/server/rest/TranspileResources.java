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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;

import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * REST resource for SQL dialect transpilation via GraalPy + sqlglot.
 * Converts SQL from one dialect (e.g. MySQL) to another (e.g. Apache Drill).
 */
@Path("/api/v1/transpile")
@Tag(name = "Transpiler", description = "SQL dialect transpilation via sqlglot")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class TranspileResources {

  // ==================== Request/Response Models ====================

  public static class TranspileRequest {
    @JsonProperty
    public String sql;

    @JsonProperty
    public String sourceDialect;

    @JsonProperty
    public String targetDialect;

    @JsonProperty
    public List<Map<String, Object>> schemas;

    public TranspileRequest() {
    }

    @JsonCreator
    public TranspileRequest(
        @JsonProperty("sql") String sql,
        @JsonProperty("sourceDialect") String sourceDialect,
        @JsonProperty("targetDialect") String targetDialect,
        @JsonProperty("schemas") List<Map<String, Object>> schemas) {
      this.sql = sql;
      this.sourceDialect = sourceDialect;
      this.targetDialect = targetDialect;
      this.schemas = schemas;
    }
  }

  public static class TranspileResponse {
    @JsonProperty
    public String sql;

    @JsonProperty
    public boolean success;

    @JsonProperty
    public String formattedOriginal;

    public TranspileResponse(String sql, boolean success) {
      this.sql = sql;
      this.success = success;
    }

    public TranspileResponse(String sql, boolean success, String formattedOriginal) {
      this.sql = sql;
      this.success = success;
      this.formattedOriginal = formattedOriginal;
    }
  }

  // ==================== Endpoints ====================

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Transpile SQL",
      description = "Transpiles SQL from one dialect to another using sqlglot")
  public Response transpile(TranspileRequest request) {
    if (request.sql == null || request.sql.trim().isEmpty()) {
      return Response.ok(new TranspileResponse("", true)).build();
    }

    SqlTranspiler transpiler = SqlTranspiler.getInstance();
    String sourceDialect = request.sourceDialect != null ? request.sourceDialect : "mysql";
    String targetDialect = request.targetDialect != null ? request.targetDialect : "drill";

    String schemasJson = "[]";
    if (request.schemas != null && !request.schemas.isEmpty()) {
      try {
        schemasJson = new ObjectMapper().writeValueAsString(request.schemas);
      } catch (Exception e) {
        schemasJson = "[]";
      }
    }

    String result = transpiler.transpile(request.sql, sourceDialect, targetDialect, schemasJson);
    return Response.ok(new TranspileResponse(result, true)).build();
  }

  // ==================== Convert Data Type ====================

  public static class ConvertDataTypeRequest {
    @JsonProperty
    public String sql;

    @JsonProperty
    public String columnName;

    @JsonProperty
    public String dataType;

    @JsonProperty
    public Map<String, Object> columns;

    public ConvertDataTypeRequest() {
    }

    @JsonCreator
    public ConvertDataTypeRequest(
        @JsonProperty("sql") String sql,
        @JsonProperty("columnName") String columnName,
        @JsonProperty("dataType") String dataType,
        @JsonProperty("columns") Map<String, Object> columns) {
      this.sql = sql;
      this.columnName = columnName;
      this.dataType = dataType;
      this.columns = columns;
    }
  }

  @POST
  @Path("/convert-type")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Convert column data type",
      description = "Wraps a column in a CAST expression using sqlglot AST manipulation")
  public Response convertDataType(ConvertDataTypeRequest request) {
    if (request.sql == null || request.sql.trim().isEmpty()) {
      return Response.ok(new TranspileResponse("", true)).build();
    }
    if (request.columnName == null || request.dataType == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new TranspileResponse(request.sql, false)).build();
    }

    SqlTranspiler transpiler = SqlTranspiler.getInstance();
    String columnsJson = null;
    if (request.columns != null && !request.columns.isEmpty()) {
      try {
        columnsJson = new ObjectMapper().writeValueAsString(request.columns);
      } catch (Exception e) {
        columnsJson = null;
      }
    }

    String result = transpiler.convertDataType(
        request.sql, request.columnName, request.dataType, columnsJson);
    if (result == null) {
      return Response.ok(new TranspileResponse(request.sql, false)).build();
    }
    String formattedOriginal = transpiler.formatSql(request.sql);
    return Response.ok(new TranspileResponse(result, true, formattedOriginal)).build();
  }

  // ==================== Change Time Grain ====================

  public static class TimeGrainRequest {
    @JsonProperty
    public String sql;

    @JsonProperty
    public String columnName;

    @JsonProperty
    public String timeGrain;

    @JsonProperty
    public List<String> columns;

    public TimeGrainRequest() {
    }

    @JsonCreator
    public TimeGrainRequest(
        @JsonProperty("sql") String sql,
        @JsonProperty("columnName") String columnName,
        @JsonProperty("timeGrain") String timeGrain,
        @JsonProperty("columns") List<String> columns) {
      this.sql = sql;
      this.columnName = columnName;
      this.timeGrain = timeGrain;
      this.columns = columns;
    }
  }

  @POST
  @Path("/time-grain")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Change time grain",
      description = "Wraps a temporal column with DATE_TRUNC using sqlglot AST manipulation")
  public Response changeTimeGrain(TimeGrainRequest request) {
    if (request.sql == null || request.sql.trim().isEmpty()) {
      return Response.ok(new TranspileResponse("", true)).build();
    }
    if (request.columnName == null || request.timeGrain == null) {
      return Response.status(Response.Status.BAD_REQUEST)
          .entity(new TranspileResponse(request.sql, false)).build();
    }

    SqlTranspiler transpiler = SqlTranspiler.getInstance();
    String columnsJson = null;
    if (request.columns != null && !request.columns.isEmpty()) {
      try {
        columnsJson = new ObjectMapper().writeValueAsString(request.columns);
      } catch (Exception e) {
        columnsJson = null;
      }
    }

    String result = transpiler.changeTimeGrain(
        request.sql, request.columnName, request.timeGrain, columnsJson);
    return Response.ok(new TranspileResponse(result, true)).build();
  }

  // ==================== Format SQL ====================

  public static class FormatRequest {
    @JsonProperty
    public String sql;

    public FormatRequest() {
    }

    @JsonCreator
    public FormatRequest(@JsonProperty("sql") String sql) {
      this.sql = sql;
    }
  }

  @POST
  @Path("/format")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Format SQL",
      description = "Pretty-prints a SQL string using sqlglot")
  public Response formatSql(FormatRequest request) {
    if (request.sql == null || request.sql.trim().isEmpty()) {
      return Response.ok(new TranspileResponse("", true)).build();
    }
    String result = SqlTranspiler.getInstance().formatSql(request.sql);
    return Response.ok(new TranspileResponse(result, true)).build();
  }

  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Get transpiler status",
      description = "Returns whether sqlglot transpilation is available")
  public Response status() {
    Map<String, Boolean> status = Collections.singletonMap(
        "available", SqlTranspiler.getInstance().isAvailable());
    return Response.ok(status).build();
  }
}
