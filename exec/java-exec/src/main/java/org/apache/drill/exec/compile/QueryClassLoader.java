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
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

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

import com.google.common.collect.MapMaker;

public class QueryClassLoader extends URLClassLoader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryClassLoader.class);

  public static final String JAVA_COMPILER_OPTION = "exec.java_compiler";
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
            .build(logger);
      }
    }
  };

  public static final String JAVA_COMPILER_DEBUG_OPTION = "exec.java_compiler_debug";
  public static final OptionValidator JAVA_COMPILER_DEBUG = new BooleanValidator(JAVA_COMPILER_DEBUG_OPTION, true);

  public static final String JAVA_COMPILER_JANINO_MAXSIZE_OPTION = "exec.java_compiler_janino_maxsize";
  public static final OptionValidator JAVA_COMPILER_JANINO_MAXSIZE = new LongValidator(JAVA_COMPILER_JANINO_MAXSIZE_OPTION, 256*1024);

  public static final String JAVA_COMPILER_CONFIG = "drill.exec.compile.compiler";
  public static final String JAVA_COMPILER_DEBUG_CONFIG = "drill.exec.compile.debug";
  public static final String JAVA_COMPILER_JANINO_MAXSIZE_CONFIG = "drill.exec.compile.janino_maxsize";

  private ClassCompilerSelector compilerSelector;

  private AtomicLong index = new AtomicLong(0);

  private ConcurrentMap<String, byte[]> customClasses = new MapMaker().concurrencyLevel(4).makeMap();

  public QueryClassLoader(DrillConfig config, OptionManager sessionOptions) {
    super(new URL[0], Thread.currentThread().getContextClassLoader());
    compilerSelector = new ClassCompilerSelector(config, sessionOptions);
  }

  public long getNextClassIndex() {
    return index.getAndIncrement();
  }

  public void injectByteCode(String className, byte[] classBytes) throws IOException {
    if (customClasses.containsKey(className)) {
      throw new IOException(String.format("The class defined %s has already been loaded.", className));
    }
    customClasses.put(className, classBytes);
  }

  @Override
  protected Class<?> findClass(String className) throws ClassNotFoundException {
    byte[] ba = customClasses.get(className);
    if (ba != null) {
      return this.defineClass(className, ba, 0, ba.length);
    }else{
      return super.findClass(className);
    }
  }

  public byte[][] getClassByteCode(final ClassNames className, final String sourceCode)
      throws CompileException, IOException, ClassNotFoundException, ClassTransformationException {
    return compilerSelector.getClassByteCode(className, sourceCode);
  }

  public enum CompilerPolicy {
    DEFAULT, JDK, JANINO;
  }

  private class ClassCompilerSelector {
    private final CompilerPolicy policy;
    private final long janinoThreshold;

    private final AbstractClassCompiler jdkClassCompiler;
    private final AbstractClassCompiler janinoClassCompiler;


    ClassCompilerSelector(DrillConfig config, OptionManager sessionOptions) {
      OptionValue value = sessionOptions.getOption(JAVA_COMPILER_OPTION);
      this.policy = CompilerPolicy.valueOf((value != null) ? value.string_val.toUpperCase() : config.getString(JAVA_COMPILER_CONFIG).toUpperCase());

      value = sessionOptions.getOption(JAVA_COMPILER_JANINO_MAXSIZE_OPTION);
      this.janinoThreshold = (value != null) ? value.num_val : config.getLong(JAVA_COMPILER_JANINO_MAXSIZE_CONFIG);

      value = sessionOptions.getOption(JAVA_COMPILER_DEBUG_OPTION);
      boolean debug = (value != null) ? value.bool_val : config.getBoolean(JAVA_COMPILER_DEBUG_CONFIG);

      this.janinoClassCompiler = (policy == CompilerPolicy.JANINO || policy == CompilerPolicy.DEFAULT) ? new JaninoClassCompiler(QueryClassLoader.this, debug) : null;
      this.jdkClassCompiler = (policy == CompilerPolicy.JDK || policy == CompilerPolicy.DEFAULT) ? JDKClassCompiler.newInstance(QueryClassLoader.this, debug) : null;
    }

    private byte[][] getClassByteCode(ClassNames className, String sourceCode)
        throws CompileException, ClassNotFoundException, ClassTransformationException, IOException {
      AbstractClassCompiler classCompiler;
      if (jdkClassCompiler != null &&
          (policy == CompilerPolicy.JDK || (policy == CompilerPolicy.DEFAULT && sourceCode.length() > janinoThreshold))) {
        classCompiler = jdkClassCompiler;
      } else {
        classCompiler = janinoClassCompiler;
      }

      byte[][] bc = classCompiler.getClassByteCode(className, sourceCode);
      /*
       * final String baseDir = System.getProperty("java.io.tmpdir") + File.separator + classCompiler.getClass().getSimpleName();
       * File classFile = new File(baseDir + className.clazz);
       * classFile.getParentFile().mkdirs();
       * BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(classFile));
       * out.write(bc[0]);
       * out.close();
       */
      return bc;
    }

  }

}
