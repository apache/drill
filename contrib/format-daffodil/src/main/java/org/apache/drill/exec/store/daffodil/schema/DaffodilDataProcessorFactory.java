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
package org.apache.drill.exec.store.daffodil.schema;


import org.apache.daffodil.japi.Compiler;
import org.apache.daffodil.japi.Daffodil;
import org.apache.daffodil.japi.DataProcessor;
import org.apache.daffodil.japi.Diagnostic;
import org.apache.daffodil.japi.InvalidParserException;
import org.apache.daffodil.japi.InvalidUsageException;
import org.apache.daffodil.japi.ProcessorFactory;
import org.apache.daffodil.japi.ValidationMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Objects;

/**
 * Compiles a DFDL schema (mostly for tests) or loads a pre-compiled DFDL schema so
 * that one can obtain a DataProcessor for use with DaffodilMessageParser.
 * <br/>
 * TODO: Needs to use a cache to avoid reloading/recompiling every time.
 */
public class DaffodilDataProcessorFactory {
  // Default constructor is used.

  private static final Logger logger = LoggerFactory.getLogger(DaffodilDataProcessorFactory.class);

  private DataProcessor dp;

  /**
   * Thrown if schema compilation fails.
   * <br/>
   * Contains diagnostic objects which give the cause(s) of the failure.
   */
  public static class CompileFailure extends Exception {
    List<Diagnostic> diags;

    CompileFailure(List<Diagnostic> diagnostics) {
      super("DFDL Schema Compile Failure");
      diags = diagnostics;
    }
  }

  /**
   * Gets a Daffodil DataProcessor given the necessary arguments to compile or reload it.
   * @param schemaFileURI pre-compiled dfdl schema (.bin extension) or DFDL schema source (.xsd extension)
   * @param validationMode Use true to request Daffodil built-in 'limited' validation.
   *                       Use false for no validation.
   * @param rootName Local name of root element of the message. Can be null to use the first element
   *                 declaration of the primary schema file. Ignored if reloading a pre-compiled schema.
   * @param rootNS Namespace URI as a string. Can be null to use the target namespace
   *               of the primary schema file or if it is unambiguous what element is the rootName.
   *               Ignored if reloading a pre-compiled schema.
   * @return the DataProcessor
   * @throws CompileFailure - if schema compilation fails
   * @throws IOException - if the schemaFileURI cannot be opened or is not found.
   * @throws URISyntaxException - if the schemaFileURI is not legal syntax.
   * @throws InvalidParserException - if the reloading of the parser from pre-compiled binary fails.
   */
   public DataProcessor getDataProcessor(
      URI schemaFileURI,
      boolean validationMode,
      String rootName,
      String rootNS)
      throws CompileFailure, IOException, URISyntaxException, InvalidParserException {

    DaffodilDataProcessorFactory dmp = new DaffodilDataProcessorFactory();
    boolean isPrecompiled = schemaFileURI.toString().endsWith(".bin");
    if (isPrecompiled) {
      if (Objects.nonNull(rootName) && !rootName.isEmpty()) {
        // A usage error. You shouldn't supply the name and optionally namespace if loading
        // precompiled schema because those are built into it. Should be null or "".
        logger.warn("Root element name '{}' is ignored when used with precompiled DFDL schema.", rootName);
      }
      dmp.loadSchema(schemaFileURI);
      dmp.setupDP(validationMode, null);
    } else {
      List<Diagnostic> pfDiags = dmp.compileSchema(schemaFileURI, rootName, rootNS);
      dmp.setupDP(validationMode, pfDiags);
    }
    return dmp.dp;
  }

  private void loadSchema(URI schemaFileURI)
      throws IOException, InvalidParserException {
    Compiler c = Daffodil.compiler();
    dp = c.reload(Channels.newChannel(schemaFileURI.toURL().openStream()));
  }

  @SuppressWarnings("ReassignedVariable")
  private List<Diagnostic> compileSchema(URI schemaFileURI, String rootName, String rootNS)
      throws URISyntaxException, IOException, CompileFailure {
    Compiler c = Daffodil.compiler();
    ProcessorFactory pf = c.compileSource(schemaFileURI, rootName, rootNS);
    List<Diagnostic> pfDiags = pf.getDiagnostics();
    if (pf.isError()) {
      pfDiags.forEach(diag ->
          logger.error(diag.getSomeMessage())
      );
      throw new CompileFailure(pfDiags);
    }
    dp = pf.onPath("/");
    return pfDiags; // must be just warnings. If it was errors we would have thrown.
  }

  /**
   * Common setup steps used whether or not we reloaded or compiled a DFDL schema.
   */
  private void setupDP(boolean validationMode, List<Diagnostic> pfDiags) throws CompileFailure {
    Objects.requireNonNull(dp); // true because failure to produce a dp throws CompileFailure.
    if (validationMode) {
      try {
        dp = dp.withValidationMode(ValidationMode.Limited);
      } catch (InvalidUsageException e) {
        // impossible
        throw new Error(e);
      }
    }
    List<Diagnostic> dpDiags = dp.getDiagnostics();
    if (dp.isError()) {
      throw new CompileFailure(dpDiags);
    }
    // well this part is only if we compiled, and provided the pfDiags arg as non-null.
    List<Diagnostic> compilationWarnings;
    if (pfDiags != null && !pfDiags.isEmpty()) {
      compilationWarnings = pfDiags;
      compilationWarnings.addAll(dpDiags); // dpDiags might be empty. That's ok.
    } else {
      compilationWarnings = dpDiags; // dpDiags might be empty. That's ok.
    }
  }
}
