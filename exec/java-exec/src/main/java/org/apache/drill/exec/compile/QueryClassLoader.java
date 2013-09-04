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
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.exec.exception.ClassTransformationException;
import org.codehaus.commons.compiler.CompileException;

import com.google.common.collect.MapMaker;

public class QueryClassLoader extends URLClassLoader {

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryClassLoader.class);

  private final ClassCompiler classCompiler;
  private AtomicLong index = new AtomicLong(0);
  private ConcurrentMap<String, byte[]> customClasses = new MapMaker().concurrencyLevel(4).makeMap();

  public QueryClassLoader(boolean useJanino) {
    super(new URL[0]);
    if (useJanino) {
      this.classCompiler = new JaninoClassCompiler(this);
    } else {
      this.classCompiler = new JDKClassCompiler();
    }
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

  public byte[] getClassByteCode(final String className, final String sourcecode) throws CompileException, IOException,
      ClassNotFoundException, ClassTransformationException {
    byte[] bc = classCompiler.getClassByteCode(className, sourcecode);
    return bc;

  }

}
