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
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.server.options.OptionManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

/**
 * Global code compiler mechanism shared by all threads and operators.
 * Holds a single cache of generated code (keyed by code source) to
 * prevent compiling identical code multiple times. Supports both
 * the byte-code merging and plain-old Java methods of code
 * generation and compilation.
 */

public class CodeCompiler {

  public static final String COMPILE_BASE = "drill.exec.compile";
  public static final String MAX_LOADING_CACHE_SIZE_CONFIG = COMPILE_BASE + ".cache_max_size";

  private final ClassTransformer transformer;
  private final ClassBuilder classBuilder;

  /**
   * Google Guava loading cache that defers creating a cache
   * entry until first needed. Creation is done in a thread-safe
   * way: if two threads try to create the same class at the same
   * time, the first does the work, the second waits for the first
   * to complete, then grabs the new entry.
   */

  private final LoadingCache<CodeGenerator<?>, GeneratedClassEntry> cache;

  public CodeCompiler(final DrillConfig config, final OptionManager optionManager) {
    transformer = new ClassTransformer(config, optionManager);
    classBuilder = new ClassBuilder(config, optionManager);
    final int cacheMaxSize = config.getInt(MAX_LOADING_CACHE_SIZE_CONFIG);
    cache = CacheBuilder.newBuilder()
        .maximumSize(cacheMaxSize)
        .build(new Loader());
  }

  /**
   * Create a single instance of the generated class.
   *
   * @param cg code generator for the class to be instantiated.
   * @return an instance of the generated class
   * @throws ClassTransformationException general "something is wrong" exception
   * for the Drill compilation chain.
   */

  @SuppressWarnings("unchecked")
  public <T> T createInstance(final CodeGenerator<?> cg) throws ClassTransformationException {
    return (T) createInstances(cg, 1).get(0);
  }

  /**
   * Create multiple instances of the generated class.
   *
   * @param cg code generator for the class to be instantiated.
   * @param count the number of instances desired.
   * @return a list of instances of the generated class.
   * @throws ClassTransformationException general "something is wrong" exception
   * for the Drill compilation chain.
   */

  @SuppressWarnings("unchecked")
  public <T> List<T> createInstances(final CodeGenerator<?> cg, int count) throws ClassTransformationException {
    cg.generate();
    try {
      final GeneratedClassEntry ce = cache.get(cg);
      List<T> tList = Lists.newArrayList();
      for ( int i = 0; i < count; i++) {
        tList.add((T) ce.clazz.newInstance());
      }
      return tList;
    } catch (ExecutionException | InstantiationException | IllegalAccessException e) {
      throw new ClassTransformationException(e);
    }
  }

  /**
   * Loader used to create an entry in the class cache when the entry
   * does not yet exist. Here, we generate the code, compile it,
   * and place the resulting class into the cache. The class has an
   * associated class loader which "dangles" from the class itself;
   * we don't keep track of the class loader itself.
   */

  private class Loader extends CacheLoader<CodeGenerator<?>, GeneratedClassEntry> {
    @Override
    public GeneratedClassEntry load(final CodeGenerator<?> cg) throws Exception {
      final Class<?> c;
      if ( cg.isPlainOldJava( ) ) {
        // Generate class as plain old Java

        c = classBuilder.getImplementationClass(cg);
      } else {
        // Generate class parts and assemble byte-codes.

        c = transformer.getImplementationClass(cg);
      }
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
