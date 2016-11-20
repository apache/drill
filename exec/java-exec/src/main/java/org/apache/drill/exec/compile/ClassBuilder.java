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
package org.apache.drill.exec.compile;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.compile.ClassTransformer.ClassNames;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.server.options.OptionManager;
import org.codehaus.commons.compiler.CompileException;

/**
 * Implements the "plain-old Java" method of code generation and
 * compilation. Given a {@link CodeGenerator}, obtains the generated
 * source code, compiles it with the selected compiler, loads the
 * byte-codes into a class loader and provides the resulting
 * class. Compared with the {@link ClassTransformer} mechanism,
 * this one requires the code generator to have generated a complete
 * Java class that is capable of direct compilation and loading.
 * This means the generated class must be a subclass of the template
 * so that the JVM can use normal Java inheritance to associate the
 * template and generated methods.
 * <p>
 * Here is how to use the plain-old Java technique to debug
 * generated code:
 * <ul>
 * <li>Set the config option <var>drill.exec.compile.save_source</var>
 * to <var>true</var>.</li>
 * <li>Set the config option <var>drill.exec.compile.code_dir</var>
 * to the location where you want to save the generated source
 * code.</li>
 * <li>Where you generate code (using a {@link CodeGenerator}),
 * set the "plain-old Java" options:<pre>
 * CodeGenerator&lt;Foo> cg = ...
 * cg.plainOldJavaCapable(true); // Class supports plain-old Java
 * cg.preferPlainOldJava(true); // Actually generate plain-old Java
 * ...</pre></li>
 * <li>In your favorite IDE, add to the code lookup path the
 * code directory saved earlier. In Eclipse, for example, you do
 * this in the debug configuration you will use to debug Drill.</li>
 * <li>Set a breakpoint in template used for the generated code.</li>
 * <li>Run Drill. The IDE will stop at your breakpoint.</li>
 * <li>Step into the generated code. Examine class field and
 * local variables. Have fun!</li>
 * </ul>
 * <p>
 * Note: not all generated code is ready to be compiled as plain-old
 * Java. Some classes omit from the template the proper <code>throws</code>
 * declarations. Other minor problems may also crop up. All are easy
 * to fix. Once you've done so, add the following to mark that you've
 * done the clean-up:<pre>
 * cg.plainOldJavaCapable(true); // Class supports plain-old Java</pre>
 * <p>
 * The setting to prefer plain-old Java is ignored for generated
 * classes not marked as plain-old Java capable.
 */

public class ClassBuilder {

  public static final String SAVE_CODE_OPTION = CodeCompiler.COMPILE_BASE + ".save_source";
  public static final String CODE_DIR_OPTION = CodeCompiler.COMPILE_BASE + ".code_dir";

  private final DrillConfig config;
  private final OptionManager options;
  private final boolean saveCode;
  private final File codeDir;

  public ClassBuilder(DrillConfig config, OptionManager optionManager) {
    this.config = config;
    options = optionManager;

    // The option to save code is a boot-time option because
    // it is used selectively during debugging, but can cause
    // excessive I/O in a running server if used to save all code.

    saveCode = config.getBoolean(SAVE_CODE_OPTION);
    codeDir = new File(config.getString(CODE_DIR_OPTION));
  }

  /**
   * Given a code generator which has already generated plain-old Java
   * code, compile the code, create a class loader, and return the
   * resulting Java class.
   *
   * @param cg a plain-old Java capable code generator that has generated
   * plain-old Java code
   * @return the class that the code generator defines
   * @throws ClassTransformationException
   */

  public Class<?> getImplementationClass(CodeGenerator<?> cg) throws ClassTransformationException {
    try {
      return compileClass(cg);
    } catch (CompileException | ClassNotFoundException|IOException e) {
      throw new ClassTransformationException(e);
    }
  }

  /**
   * Performs the actual work of compiling the code and loading the class.
   *
   * @param cg the code generator that has built the class(es) to be generated.
   * @return the class, after code generation and (if needed) compilation.
   * @throws IOException if an error occurs when optionally writing code to disk.
   * @throws CompileException if the generated code has compile issues.
   * @throws ClassNotFoundException if the generated code references unknown classes.
   * @throws ClassTransformationException generic "something is wrong" error from
   * Drill class compilation code.
   */
  private Class<?> compileClass(CodeGenerator<?> cg) throws IOException, CompileException, ClassNotFoundException, ClassTransformationException {

    // Get the plain-old Java code.

    String code = cg.getGeneratedCode();

    // Get the class names (dotted, file path, etc.)

    String className = cg.getMaterializedClassName();
    ClassTransformer.ClassNames name = new ClassTransformer.ClassNames(className);

    // A key advantage of this method is that the code can be
    // saved and debugged, if needed.

    saveCode(code, name);

    // Compile the code and load it into a class loader.

    CachedClassLoader classLoader = new CachedClassLoader();
    ClassCompilerSelector compilerSelector = new ClassCompilerSelector(classLoader, config, options);
    Map<String,byte[]> results = compilerSelector.compile(name, code);
    classLoader.addClasses(results);

    // Get the class from the class loader.

    try {
      return classLoader.findClass(className);
    } catch (ClassNotFoundException e) {
      // This should never occur.
      throw new IllegalStateException("Code load failed", e);
    }
  }

  /**
   * Save code to a predefined location for debugging. To use the code
   * for debugging, make sure the save location is on your IDE's source
   * code search path. Code is saved in usual Java format with each
   * package as a directory. The provided code directory becomes a
   * source directory, as in Maven's "src/main/java".
   *
   * @param code the source code
   * @param name the class name
   */

  private void saveCode(String code, ClassNames name) {

    // Skip if we don't want to save the code.

    if (! saveCode) { return; }

    String pathName = name.slash + ".java";
    File codeFile = new File(codeDir, pathName);
    codeFile.getParentFile().mkdirs();
    try (final FileWriter writer = new FileWriter(codeFile)) {
      writer.write(code);
    } catch (IOException e) {
      System.err.println("Could not save: " + codeFile.getAbsolutePath());
    }
  }
}
