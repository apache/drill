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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.compile.ClassTransformer.ClassNames;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValidator;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.TypeValidators.BooleanValidator;
import org.apache.drill.exec.server.options.TypeValidators.LongValidator;
import org.apache.drill.exec.server.options.TypeValidators.StringValidator;
import org.codehaus.commons.compiler.CompileException;

/**
 * Selects between the two supported Java compilers: Janino and
 * the build-in Java compiler.
 *
 * <h4>Session Options</h4>
 * <dl>
 * <dt>exec.java_compiler</dt>
 * <dd>The compiler to use. Valid options are defined in the
 * {@link ClassCompilerSelector.CompilerPolicy} enum.</dd>
 * <dt>exec.java_compiler_debug</dt>
 * <dd>If debug logging is enabled, then {@link AbstractClassCompiler} writes the
 * generated Java code to the log file prior to compilation. This option
 * adds line numbers to the logged code.</dd>
 * <dt>exec.java_compiler_janino_maxsize</dt>
 * <dd>The maximum size of code that the Janio compiler can handle. Larger code is
 * handled by the JDK compiler. Defaults to 256K.</dd>
 * </dl>
 * <h4>Configuration Options</h4>
 * Configuration options are used when the above session options are unset.
 * <dl>
 * <dt>drill.exec.compile.compiler</dt>
 * <dd>Default for <var>exec.java_compiler</var></dd>
 * <dt>drill.exec.compile.debug</dt>
 * <dd>Default for <var>exec.java_compiler_debug</var></dd>
 * <dt>drill.exec.compile.janino_maxsize</dt>
 * <dd>Default for <var>exec.java_compiler_janino_maxsize</var></dd>
 * </dl>
 */

public class ClassCompilerSelector {
  public enum CompilerPolicy {
    DEFAULT, JDK, JANINO;
  }

  public static final String JAVA_COMPILER_JANINO_MAXSIZE_CONFIG = CodeCompiler.COMPILE_BASE + ".janino_maxsize";
  public static final String JAVA_COMPILER_DEBUG_CONFIG = CodeCompiler.COMPILE_BASE + ".debug";
  public static final String JAVA_COMPILER_CONFIG = CodeCompiler.COMPILE_BASE + ".compiler";

  public static final String JAVA_COMPILER_OPTION = "exec.java_compiler";
  public static final String JAVA_COMPILER_JANINO_MAXSIZE_OPTION = "exec.java_compiler_janino_maxsize";
  public static final OptionValidator JAVA_COMPILER_JANINO_MAXSIZE = new LongValidator(JAVA_COMPILER_JANINO_MAXSIZE_OPTION, 256*1024);

  public static final String JAVA_COMPILER_DEBUG_OPTION = "exec.java_compiler_debug";
  public static final OptionValidator JAVA_COMPILER_DEBUG = new BooleanValidator(JAVA_COMPILER_DEBUG_OPTION, true);

  public static final StringValidator JAVA_COMPILER_VALIDATOR = new StringValidator(JAVA_COMPILER_OPTION, CompilerPolicy.DEFAULT.toString()) {
    @Override
    public void validate(final OptionValue v, final OptionManager manager) {
      super.validate(v, manager);
      try {
        CompilerPolicy.valueOf(v.string_val.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw UserException.validationError()
            .message("Invalid value '%s' specified for option '%s'. Valid values are %s.",
              v.string_val, getOptionName(), Arrays.toString(CompilerPolicy.values()))
            .build(QueryClassLoader.logger);
      }
    }
  };

  private final CompilerPolicy policy;
  private final long janinoThreshold;

  private final AbstractClassCompiler jdkClassCompiler;
  private final AbstractClassCompiler janinoClassCompiler;

  public ClassCompilerSelector(ClassLoader classLoader, DrillConfig config, OptionManager sessionOptions) {
    OptionValue value = sessionOptions.getOption(JAVA_COMPILER_OPTION);
    this.policy = CompilerPolicy.valueOf((value != null) ? value.string_val.toUpperCase() : config.getString(JAVA_COMPILER_CONFIG).toUpperCase());

    value = sessionOptions.getOption(JAVA_COMPILER_JANINO_MAXSIZE_OPTION);
    this.janinoThreshold = (value != null) ? value.num_val : config.getLong(JAVA_COMPILER_JANINO_MAXSIZE_CONFIG);

    value = sessionOptions.getOption(JAVA_COMPILER_DEBUG_OPTION);
    boolean debug = (value != null) ? value.bool_val : config.getBoolean(JAVA_COMPILER_DEBUG_CONFIG);

    this.janinoClassCompiler = (policy == CompilerPolicy.JANINO || policy == CompilerPolicy.DEFAULT) ? new JaninoClassCompiler(classLoader, debug) : null;
    this.jdkClassCompiler = (policy == CompilerPolicy.JDK || policy == CompilerPolicy.DEFAULT) ? JDKClassCompiler.newInstance(classLoader, debug) : null;
  }

  byte[][] getClassByteCode(ClassNames className, String sourceCode)
      throws CompileException, ClassNotFoundException, ClassTransformationException, IOException {

    byte[][] bc = getCompiler(sourceCode).getClassByteCode(className, sourceCode);

    // Uncomment the following to save the generated byte codes.

//    final String baseDir = System.getProperty("java.io.tmpdir") + File.separator + className;
//    File classFile = new File(baseDir + className.clazz);
//    classFile.getParentFile().mkdirs();
//    try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(classFile))) {
//      out.write(bc[0]);
//    }

    return bc;
  }

  public Map<String,byte[]> compile(ClassNames className, String sourceCode)
      throws CompileException, ClassNotFoundException, ClassTransformationException, IOException {
    return getCompiler(sourceCode).compile(className, sourceCode);
  }

  private AbstractClassCompiler getCompiler(String sourceCode) {
    if (jdkClassCompiler != null &&
        (policy == CompilerPolicy.JDK || (policy == CompilerPolicy.DEFAULT && sourceCode.length() > janinoThreshold))) {
      return jdkClassCompiler;
    } else {
      return janinoClassCompiler;
    }
  }
}