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

import org.apache.drill.common.exceptions.ExpressionParsingException;
import org.apache.drill.exec.compile.ClassTransformer.ClassNames;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValidator;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.server.options.TypeValidators.LongValidator;
import org.apache.drill.exec.server.options.TypeValidators.StringValidator;
import org.codehaus.commons.compiler.CompileException;
import org.eigenbase.sql.SqlLiteral;

import com.google.common.collect.MapMaker;

public class QueryClassLoader extends URLClassLoader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryClassLoader.class);

  public static final String JAVA_COMPILER_OPTION = "exec.java_compiler";
  public static final StringValidator JAVA_COMPILER_VALIDATOR = new StringValidator(JAVA_COMPILER_OPTION, CompilerPolicy.DEFAULT.toString()) {
    @Override
    public OptionValue validate(SqlLiteral value) throws ExpressionParsingException {
      OptionValue ov = super.validate(value);
      try {
        CompilerPolicy.valueOf(ov.string_val.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new ExpressionParsingException(String.format("Invalid value '%s' specified for option '%s'. Valid values are %s.",
            ov.string_val, getOptionName(), Arrays.toString(CompilerPolicy.values())));
      }
      return ov;
    }
  };

  public static final String JAVA_COMPILER_JANINO_MAXSIZE_OPTION = "exec.java_compiler_janino_maxsize";
  public static final OptionValidator JAVA_COMPILER_JANINO_MAXSIZE = new LongValidator(JAVA_COMPILER_JANINO_MAXSIZE_OPTION, 256*1024);

  private ClassCompilerSelector compilerSelector;

  private AtomicLong index = new AtomicLong(0);
  
  private ConcurrentMap<String, byte[]> customClasses = new MapMaker().concurrencyLevel(4).makeMap();

  public QueryClassLoader(OptionManager sessionOptions) {
    super(new URL[0]);
    compilerSelector = new ClassCompilerSelector(sessionOptions);
  }

  public long getNextClassIndex(){
    return index.getAndIncrement();
  }

  public void injectByteCode(String className, byte[] classBytes) throws IOException {
    if(customClasses.containsKey(className)) throw new IOException(String.format("The class defined {} has already been loaded.", className));
    customClasses.put(className, classBytes);
  }

  @Override
  protected Class<?> findClass(String className) throws ClassNotFoundException {
    byte[] ba = customClasses.get(className);
    if(ba != null){
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

    private ClassCompiler jdkClassCompiler;
    private ClassCompiler janinoClassCompiler;


    ClassCompilerSelector(OptionManager sessionOptions) {
      OptionValue value = sessionOptions.getOption(JAVA_COMPILER_OPTION);
      this.policy = (value != null) ? CompilerPolicy.valueOf(value.string_val.toUpperCase()) : CompilerPolicy.DEFAULT;
      value = sessionOptions.getOption(JAVA_COMPILER_JANINO_MAXSIZE_OPTION);
      this.janinoThreshold = (value != null) ? value.num_val : JAVA_COMPILER_JANINO_MAXSIZE.getDefault().num_val;

      this.janinoClassCompiler = new JaninoClassCompiler(QueryClassLoader.this);
      this.jdkClassCompiler = new JDKClassCompiler(QueryClassLoader.this);
    }

    private byte[][] getClassByteCode(ClassNames className, String sourceCode)
        throws CompileException, ClassNotFoundException, ClassTransformationException, IOException {
      ClassCompiler classCompiler;
      if (policy == CompilerPolicy.JDK || (policy == CompilerPolicy.DEFAULT && sourceCode.length() > janinoThreshold)) {
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
