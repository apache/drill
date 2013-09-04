/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.compile;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticListener;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.Location;
import org.codehaus.commons.compiler.jdk.ByteArrayJavaFileManager;
import org.codehaus.commons.compiler.jdk.ByteArrayJavaFileManager.ByteArrayJavaFileObject;

import com.google.common.collect.Lists;


class JDKClassCompiler implements ClassCompiler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JDKClassCompiler.class);

  private boolean debugLines;
  private boolean debugVars;
  private boolean debugSource;
  private Collection<String> compilerOptions = new ArrayList<String>();
  private DiagnosticListener<JavaFileObject> listener;
  private final JavaCompiler compiler;
  private final JavaFileManager fileManager;
  
  public JDKClassCompiler() {
    this.compiler = ToolProvider.getSystemJavaCompiler();
    if (compiler == null) {
      throw new UnsupportedOperationException(
          "JDK Java compiler not available - probably you're running a JRE, not a JDK");
    }
    this.fileManager = new ByteArrayJavaFileManager<JavaFileManager>(compiler.getStandardFileManager(null, null, null));
    this.listener = new DiagListener();
  }

  private JavaFileObject getCompilationUnit(final String s) {

    final URI uri;
    try {
      uri = new URI("drill-class-compiler");
    } catch (URISyntaxException use) {
      throw new RuntimeException(use);
    }
    JavaFileObject javaFileObject = new SimpleJavaFileObject(uri, Kind.SOURCE) {

      @Override
      public boolean isNameCompatible(String simpleName, Kind kind) {
        return true;
      }

      @Override
      public Reader openReader(boolean ignoreEncodingErrors) throws IOException {
        return new StringReader(s);
      }

      @Override
      public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
        return s;
      }

    };

    return javaFileObject;
  }

  private List<String> getOptions() {
    List<String> opts = Lists.newArrayList(compilerOptions);
    String option = this.debugSource ? "-g:source" + (this.debugLines ? ",lines" : "")
        + (this.debugVars ? ",vars" : "") : this.debugLines ? "-g:lines" + (this.debugVars ? ",vars" : "")
        : this.debugVars ? "-g:vars" : "-g:none";
    opts.add(option);
    return opts;
  }

  /* (non-Javadoc)
   * @see org.apache.drill.exec.compile.ClassCompiler#getClassByteCode(java.lang.String, java.lang.String)
   */
  @Override
  public byte[] getClassByteCode(final String className, final String sourcecode) throws CompileException, IOException,
      ClassNotFoundException {

    // Create one Java source file in memory, which will be compiled later.
    JavaFileObject compilationUnit = getCompilationUnit(sourcecode);

    //logger.debug("Compiling the following source code\n{}", sourcecode);
    // Run the compiler.
    try {
      CompilationTask task = compiler.getTask(null, fileManager, listener, getOptions(), null, Collections.singleton(compilationUnit));
      long n0 = System.nanoTime();
      if(!task.call()){
        throw new CompileException("Compilation failed", null);
      }
      long n1 = (System.nanoTime() - n0)/1000/1000;
      
    } catch (RuntimeException rte) {
      
      // Unwrap the compilation exception and throw it.
      Throwable cause = rte.getCause();
      if (cause != null) {
        cause = cause.getCause();
        if (cause instanceof CompileException) throw (CompileException) cause;
        if (cause instanceof IOException) throw (IOException) cause;
      }
      throw rte;
    }

    JavaFileObject classFileObject = fileManager.getJavaFileForInput(StandardLocation.CLASS_OUTPUT, className, Kind.CLASS);

    if (classFileObject == null) {
      throw new ClassNotFoundException(className + ": Class file not created by compilation");
    }

    if (!(classFileObject instanceof ByteArrayJavaFileObject))
      throw new UnsupportedOperationException("Only supports byte array based java file objects.");

    ByteArrayJavaFileObject bajfo = (ByteArrayJavaFileObject) classFileObject;
    return bajfo.toByteArray();

  }

  private class DiagListener implements DiagnosticListener<JavaFileObject> {
    @Override
    public void report(Diagnostic<? extends JavaFileObject> diagnostic) {
      System.err.println("*** " + diagnostic.toString() + " *** " + diagnostic.getCode());

      Location loc = new Location( //
          diagnostic.getSource().toString(), //
          (short) diagnostic.getLineNumber(), //
          (short) diagnostic.getColumnNumber() //
      );
      String code = diagnostic.getCode();
      String message = diagnostic.getMessage(null) + " (" + code + ")";

      // Wrap the exception in a RuntimeException, because "report()" does not declare checked
      // exceptions.
      throw new RuntimeException(new CompileException(message, loc));
    }
  }


  
  

}
