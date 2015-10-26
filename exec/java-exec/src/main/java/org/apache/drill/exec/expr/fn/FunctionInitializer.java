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
package org.apache.drill.exec.expr.fn;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.util.FileUtils;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.Java.CompilationUnit;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.mortbay.util.IO;

import com.google.common.collect.Maps;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;

/**
 * To avoid the cost of initializing all functions up front,
 * this class contains all informations required to initializing a function when it is used.
 */
public class FunctionInitializer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionInitializer.class);

  private String className;

  private Map<String, CompilationUnit> functionUnits = Maps.newHashMap();
  private Map<String, String> methods;
  private List<String> imports;
  private volatile boolean ready;

  /**
   * @param className the fully qualified name of the class implementing the function
   */
  public FunctionInitializer(String className) {
    super();
    this.className = className;
  }

  /**
   * @return the fully qualified name of the class implementing the function
   */
  public String getClassName() {
    return className;
  }

  /**
   * @return the imports of this class (for java code gen)
   */
  public List<String> getImports() {
    checkInit();
    return imports;
  }

  /**
   * @param methodName
   * @return the content of the method (for java code gen inlining)
   */
  public String getMethod(String methodName) {
    checkInit();
    return methods.get(methodName);
  }

  private void checkInit() {
    if (ready) {
      return;
    }

    synchronized (this) {
      if (ready) {
        return;
      }

      // get function body.

      try {
        final Class<?> clazz = Class.forName(className);
        final CompilationUnit cu = get(clazz);

        if (cu == null) {
          throw new IOException(String.format("Failure while loading class %s.", clazz.getName()));
        }

        methods = MethodGrabbingVisitor.getMethods(cu, clazz);
        imports = ImportGrabber.getMethods(cu);

      } catch (IOException | ClassNotFoundException e) {
        throw UserException.functionError(e)
            .message("Failure reading Function class.")
            .addContext("Function Class", className)
            .build(logger);
      }
    }
  }

  private CompilationUnit get(Class<?> c) throws IOException {
    String path = c.getName();
    path = path.replaceFirst("\\$.*", "");
    path = path.replace(".", FileUtils.separator);
    path = "/" + path + ".java";
    CompilationUnit cu = functionUnits.get(path);
    if (cu != null) {
      return cu;
    }

    URL u = Resources.getResource(c, path);
    InputSupplier<InputStream> supplier = Resources.newInputStreamSupplier(u);
    try (InputStream is = supplier.getInput()) {
      if (is == null) {
        throw new IOException(String.format(
            "Failure trying to located source code for Class %s, tried to read on classpath location %s", c.getName(),
            path));
      }
      String body = IO.toString(is);

      // TODO: Hack to remove annotations so Janino doesn't choke. Need to reconsider this problem...
      body = body.replaceAll("@\\w+(?:\\([^\\\\]*?\\))?", "");
      try {
        cu = new Parser(new Scanner(null, new StringReader(body))).parseCompilationUnit();
        functionUnits.put(path, cu);
        return cu;
      } catch (CompileException e) {
        logger.warn("Failure while parsing function class:\n{}", body, e);
        return null;
      }

    }

  }

}