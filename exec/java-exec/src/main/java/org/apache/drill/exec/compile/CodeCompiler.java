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
import java.util.concurrent.ExecutionException;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.store.sys.local.LocalPStoreProvider;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class CodeCompiler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CodeCompiler.class);

  private final ClassTransformer transformer;
  private final LoadingCache<CodeGenerator<?>, GeneratedClassEntry>  cache;
  private final DrillConfig config;
  private final OptionManager systemOptionManager;

  public CodeCompiler(DrillConfig config, OptionManager systemOptionManager){
    this.transformer = new ClassTransformer();
    int cacheMaxSize = config.getInt(ExecConstants.MAX_LOADING_CACHE_SIZE_CONFIG);
    this.cache = CacheBuilder //
        .newBuilder() //
        .maximumSize(cacheMaxSize) //
        .build(new Loader());
    this.systemOptionManager = systemOptionManager;
    this.config = config;
  }

  @SuppressWarnings("unchecked")
  public <T> T getImplementationClass(CodeGenerator<?> cg) throws ClassTransformationException, IOException {
    cg.generate();
    try {
      GeneratedClassEntry ce = cache.get(cg);
      return (T) ce.clazz.newInstance();
    } catch (ExecutionException | InstantiationException | IllegalAccessException e) {
      throw new ClassTransformationException(e);
    }
  }

  private class Loader extends CacheLoader<CodeGenerator<?>, GeneratedClassEntry>{
    @Override
    public GeneratedClassEntry load(CodeGenerator<?> cg) throws Exception {
      QueryClassLoader loader = new QueryClassLoader(config, systemOptionManager);
      Class<?> c = transformer.getImplementationClass(loader, cg.getDefinition(), cg.getGeneratedCode(), cg.getMaterializedClassName());
      return new GeneratedClassEntry(loader, c);
    }
  }

  private class GeneratedClassEntry {

    private final QueryClassLoader classLoader;
    private final Class<?> clazz;

    public GeneratedClassEntry(QueryClassLoader classLoader, Class<?> c) {
      super();
      this.classLoader = classLoader;
      this.clazz = c;
    }

  }

  public static CodeCompiler getTestCompiler(DrillConfig c) throws IOException{
    return new CodeCompiler(c, new SystemOptionManager(c, new LocalPStoreProvider(c)).init());
  }
}
