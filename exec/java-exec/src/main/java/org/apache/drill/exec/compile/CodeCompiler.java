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
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.store.sys.local.LocalPStoreProvider;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

public class CodeCompiler {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CodeCompiler.class);

  private final ClassTransformer transformer;
  private final LoadingCache<CodeGenerator<?>, GeneratedClassEntry> cache;
  private final DrillConfig config;
  private final OptionManager optionManager;

  public CodeCompiler(final DrillConfig config, final OptionManager optionManager) {
    transformer = new ClassTransformer(optionManager);
    final int cacheMaxSize = config.getInt(ExecConstants.MAX_LOADING_CACHE_SIZE_CONFIG);
    cache = CacheBuilder.newBuilder()
        .maximumSize(cacheMaxSize)
        .build(new Loader());
    this.optionManager = optionManager;
    this.config = config;
  }

  @SuppressWarnings("unchecked")
  public <T> T getImplementationClass(final CodeGenerator<?> cg) throws ClassTransformationException, IOException {
    return (T) getImplementationClass(cg, 1).get(0);
  }

  @SuppressWarnings("unchecked")
  public <T> List<T> getImplementationClass(final CodeGenerator<?> cg, int instanceNumber) throws ClassTransformationException, IOException {
    cg.generate();
    try {
      final GeneratedClassEntry ce = cache.get(cg);
      List<T> tList = Lists.newArrayList();
      for ( int i = 0; i < instanceNumber; i++) {
        tList.add((T) ce.clazz.newInstance());
      }
      return tList;
    } catch (ExecutionException | InstantiationException | IllegalAccessException e) {
      throw new ClassTransformationException(e);
    }
  }

  private class Loader extends CacheLoader<CodeGenerator<?>, GeneratedClassEntry> {
    @Override
    public GeneratedClassEntry load(final CodeGenerator<?> cg) throws Exception {
      final QueryClassLoader loader = new QueryClassLoader(config, optionManager);
      final Class<?> c = transformer.getImplementationClass(loader, cg.getDefinition(),
          cg.getGeneratedCode(), cg.getMaterializedClassName());
      return new GeneratedClassEntry(c);
    }
  }

  private class GeneratedClassEntry {
    private final Class<?> clazz;

    public GeneratedClassEntry(final Class<?> clazz) {
      this.clazz = clazz;
    }
  }

  /**
   * Flush the compiled classes from the cache.
   *
   * <p>The cache has DrillbitContext lifetime, so the only way items go out of it
   * now is by being aged out because of the maximum cache size.
   *
   * <p>The intent of flushCache() is to make it possible to flush the cache for
   * testing purposes, although this could be used by users in case issues arise. If
   * that happens, remove the visible for testing annotation.
   */
  @VisibleForTesting
  public void flushCache() {
    cache.invalidateAll();
  }
}
