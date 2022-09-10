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
package org.apache.drill.exec.compile;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.drill.exec.compile.ClassTransformer.ClassNames;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassLoaderIClassLoader;
import org.codehaus.janino.IClassLoader;
import org.codehaus.janino.Java;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.codehaus.janino.UnitCompiler;
import org.codehaus.janino.util.ClassFile;

class JaninoClassCompiler extends AbstractClassCompiler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JaninoClassCompiler.class);

  private final IClassLoader compilationClassLoader;

  public JaninoClassCompiler(ClassLoader parentClassLoader, boolean debug) {
    super(debug);
    this.compilationClassLoader = new ClassLoaderIClassLoader(parentClassLoader);
  }

  @Override
  protected byte[][] getByteCode(final ClassNames className, final String sourceCode)
      throws CompileException, IOException {
    List<ClassFile> classFiles = doCompile(sourceCode);

    byte[][] byteCodes = new byte[classFiles.size()][];
    for(int i = 0; i < classFiles.size(); i++){
      byteCodes[i] = classFiles.get(i).toByteArray();
    }
    return byteCodes;
  }

  @Override
  public Map<String,byte[]> compile(final ClassNames className, final String sourceCode)
      throws CompileException, IOException, ClassNotFoundException {

    return doCompile(sourceCode).stream()
      .collect(Collectors.toMap(ClassFile::getThisClassName, ClassFile::toByteArray, (a, b) -> b));
  }

  private List<ClassFile> doCompile(final String sourceCode)
      throws CompileException, IOException {
    StringReader reader = new StringReader(sourceCode);
    Scanner scanner = new Scanner(null, reader);
    Java.AbstractCompilationUnit compilationUnit = new Parser(scanner).parseAbstractCompilationUnit();
    List<ClassFile> classFiles = new ArrayList<>();
    new UnitCompiler(compilationUnit, compilationClassLoader)
      .compileUnit(this.debug, this.debug, this.debug, classFiles);
    return classFiles;
  }

  @Override
  protected org.slf4j.Logger getLogger() { return logger; }
}
