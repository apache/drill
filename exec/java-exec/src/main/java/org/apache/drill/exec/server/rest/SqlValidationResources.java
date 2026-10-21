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
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.planner.sql.DrillConformance;
import org.apache.drill.exec.planner.sql.parser.impl.DrillParserWithCompoundIdConverter;
import org.apache.drill.exec.server.rest.auth.DrillUserPrincipal;

import jakarta.annotation.security.RolesAllowed;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * REST resource for real-time SQL syntax validation.
 * Uses Drill's Calcite-based SQL parser to check syntax without executing.
 */
@Path("/api/v1/query/validate")
@Tag(name = "Query Validation", description = "Real-time SQL syntax validation")
@RolesAllowed(DrillUserPrincipal.AUTHENTICATED_ROLE)
public class SqlValidationResources {

  /** Parser config matching SqlConverter.java to accept the same SQL dialect. */
  private static final SqlParser.Config PARSER_CONFIG = SqlParser.Config.DEFAULT
      .withIdentifierMaxLength(PlannerSettings.DEFAULT_IDENTIFIER_MAX_LENGTH)
      .withParserFactory(DrillParserWithCompoundIdConverter.FACTORY)
      .withCaseSensitive(false)
      .withConformance(new DrillConformance())
      .withUnquotedCasing(Casing.UNCHANGED)
      .withQuotedCasing(Casing.UNCHANGED);

  // ==================== Request/Response Models ====================

  public static class ValidateRequest {
    @JsonProperty
    public String sql;

    public ValidateRequest() {
    }

    @JsonCreator
    public ValidateRequest(@JsonProperty("sql") String sql) {
      this.sql = sql;
    }
  }

  public static class ValidationError {
    @JsonProperty
    public String message;

    @JsonProperty
    public int line;

    @JsonProperty
    public int column;

    @JsonProperty
    public int endLine;

    @JsonProperty
    public int endColumn;

    @JsonProperty
    public String severity;

    public ValidationError(String message, int line, int column,
        int endLine, int endColumn, String severity) {
      this.message = message;
      this.line = line;
      this.column = column;
      this.endLine = endLine;
      this.endColumn = endColumn;
      this.severity = severity;
    }
  }

  public static class ValidateResponse {
    @JsonProperty
    public boolean valid;

    @JsonProperty
    public List<ValidationError> errors;

    public ValidateResponse(boolean valid, List<ValidationError> errors) {
      this.valid = valid;
      this.errors = errors;
    }
  }

  // ==================== Endpoint ====================

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Validate SQL syntax",
      description = "Parses SQL using Drill's Calcite parser and returns syntax errors")
  public Response validate(ValidateRequest request) {
    if (request.sql == null || request.sql.trim().isEmpty()) {
      return Response.ok(
          new ValidateResponse(true, Collections.emptyList())).build();
    }

    // Strip trailing semicolons (Drill's parser doesn't accept them)
    String sql = request.sql.replaceAll(";\\s*$", "");

    try {
      SqlParser.create(sql, PARSER_CONFIG).parseStmt();
      return Response.ok(
          new ValidateResponse(true, Collections.emptyList())).build();
    } catch (SqlParseException e) {
      String msg = e.getMessage();
      SqlParserPos pos = e.getPos();

      // Determine severity: EOF errors are warnings (user still typing)
      String severity = (msg != null && msg.contains("<EOF>"))
          ? "warning" : "error";

      int line = pos != null ? pos.getLineNum() : 1;
      int column = pos != null ? pos.getColumnNum() : 1;
      int endLine = pos != null ? pos.getEndLineNum() : line;
      int endColumn = pos != null ? pos.getEndColumnNum() : column;

      // Clean up error message: take the first line for readability
      String cleanMsg = msg != null ? msg.split("\n")[0] : "Syntax error";

      List<ValidationError> errors = new ArrayList<>();
      errors.add(new ValidationError(
          cleanMsg, line, column, endLine, endColumn, severity));

      return Response.ok(new ValidateResponse(false, errors)).build();
    }
  }
}
