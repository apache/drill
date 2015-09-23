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
package org.apache.drill.common.util;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.CommonConstants;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ConfigurationBuilder;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;

public class PathScanner {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PathScanner.class);

  private static final SubTypesScanner subTypeScanner = new SubTypesScanner();
  private static final TypeAnnotationsScanner annotationsScanner = new TypeAnnotationsScanner();
  private static final ResourcesScanner resourcesScanner = new ResourcesScanner();
  private static final Object SYNC = new Object();
  static volatile Reflections REFLECTIONS = null;

  public static <A extends Annotation, T> Map<A, Class<? extends T>> scanForAnnotatedImplementations(
      Class<A> annotationClass, Class<T> baseClass, final List<String> scanPackages) {
    final Collection<Class<? extends T>> providerClasses =
        scanForImplementations(baseClass, scanPackages);

    final Map<A, Class<? extends T>> map = new HashMap<A, Class<? extends T>>();

    for (Class<? extends T> c : providerClasses) {
      final A annotation = (A) c.getAnnotation(annotationClass);
      if (annotation == null) {
        continue;
      }
      map.put(annotation, c);
    }

    return map;
  }

  private static Reflections getReflections() {
    if (REFLECTIONS == null) {
      REFLECTIONS =
          new Reflections(
              new ConfigurationBuilder()
                  .setUrls(getMarkedPaths())
                  .setScanners(subTypeScanner, annotationsScanner, resourcesScanner));
    }
    return REFLECTIONS;
  }

  /**
   * @param  scanPackages  note:  not currently used
   */
  public static <T> Class<?>[] scanForImplementationsArr(final Class<T> baseClass,
                                                         final List<String> scanPackages) {
    Collection<Class<? extends T>> imps = scanForImplementations(baseClass, scanPackages);
    Class<?>[] arr = imps.toArray(new Class<?>[imps.size()]);
    return arr;
  }

  /**
   * @param  scanPackages  note:  not currently used
   */
  public static <T> Set<Class<? extends T>> scanForImplementations(final Class<T> baseClass,
                                                                   final List<String> scanPackages) {
    final Stopwatch w = new Stopwatch().start();
    try {
      synchronized(SYNC) {
        final Set<Class<? extends T>> classes = getReflections().getSubTypesOf(baseClass);
        for (Iterator<Class<? extends T>> i = classes.iterator(); i.hasNext();) {
          final Class<? extends T> c = i.next();
          assert baseClass.isAssignableFrom(c);
          if (Modifier.isAbstract(c.getModifiers())) {
            i.remove();
          }
        }
        return classes;
      }
    } finally {
      logger.debug("Implementations scanning took {} ms for {}.",
                   w.elapsed(TimeUnit.MILLISECONDS),
                   baseClass); // no .getName(), so it has "class "/"interface "
    }
  }

  private static Collection<URL> getMarkedPaths() {
    Collection<URL> urls =
        forResource(CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME, true);
    return urls;
  }

  public static Collection<URL> getConfigURLs() {
    return forResource(CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME, false);
  }

  /**
   * Gets URLs of any classpath resources with given resource pathname.
   *
   * @param  resourcePathname  resource pathname of classpath resource instances
   *           to scan for (relative to specified class loaders' classpath roots)
   * @param  returnRootPathname  whether to collect classpath root portion of
   *           URL for each resource instead of full URL of each resource
   * @param  classLoaders  not currently used (was: "set of class loaders in
   *           which to look up resource; none (empty array) to specify to use
   *           current thread's context class loader and {@link Reflections}'s
   *           class loader")
   * @returns  ...; empty set if none
   */
  public static Set<URL> forResource(final String resourcePathname,
                                     final boolean returnRootPathname,
                                     final ClassLoader... classLoaders) {
    logger.debug("Scanning classpath for resources with pathname \"{}\".",
                 resourcePathname);
    final Set<URL> resultUrlSet = Sets.newHashSet();

    final ClassLoader classLoader = PathScanner.class.getClassLoader();
    try {
      final Enumeration<URL> resourceUrls =
          classLoader.getResources(resourcePathname);
      while (resourceUrls.hasMoreElements()) {
        final URL resourceUrl = resourceUrls.nextElement();
        logger.trace("- found a(n) {} at {}.", resourcePathname, resourceUrl);

        int index = resourceUrl.toExternalForm().lastIndexOf(resourcePathname);
        if (index != -1 && returnRootPathname) {
          final URL classpathRootUrl =
              new URL(resourceUrl.toExternalForm().substring(0, index));
          resultUrlSet.add(classpathRootUrl);
          logger.debug("- collected resource's classpath root URL {}.",
              classpathRootUrl);
        } else {
          resultUrlSet.add(resourceUrl);
          logger.debug("- collected resource URL {}.", resourceUrl);
        }
      }
    } catch (IOException e) {
      if (Reflections.log != null) {
        Reflections.log.error(
            "Error scanning for resources named " + resourcePathname, e);
      }
    }

    return resultUrlSet;
  }

}
