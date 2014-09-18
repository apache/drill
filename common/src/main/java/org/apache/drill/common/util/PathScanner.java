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

import org.apache.drill.common.config.CommonConstants;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import com.google.common.collect.Sets;

public class PathScanner {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PathScanner.class);

  private static final SubTypesScanner subTypeScanner = new SubTypesScanner();
  private static final TypeAnnotationsScanner annotationsScanner = new TypeAnnotationsScanner();
  private static final ResourcesScanner resourcesScanner = new ResourcesScanner();
  private static final Object SYNC = new Object();
  static volatile Reflections REFLECTIONS = null;

  public static <A extends Annotation, T> Map<A, Class<? extends T>> scanForAnnotatedImplementations(Class<A> annotationClass, Class<T> baseClass, final List<String> scanPackages) {
    Collection<Class<? extends T>> providerClasses = scanForImplementations(baseClass, scanPackages);

    Map<A, Class<? extends T>> map = new HashMap<A, Class<? extends T>>();

    for (Class<? extends T> c : providerClasses) {
      A annotation = (A) c.getAnnotation(annotationClass);
      if (annotation == null) {
        continue;
      }
      map.put(annotation, c);
    }

    return map;
  }

  private static Reflections getReflections() {
    if (REFLECTIONS == null) {
      REFLECTIONS = new Reflections(new ConfigurationBuilder().setUrls(getMarkedPaths()).setScanners(subTypeScanner, annotationsScanner, resourcesScanner));
    }
    return REFLECTIONS;
  }

  public static <T> Class<?>[] scanForImplementationsArr(Class<T> baseClass, final List<String> scanPackages) {
    Collection<Class<? extends T>> imps = scanForImplementations(baseClass, scanPackages);
    return imps.toArray(new Class<?>[imps.size()]);
  }

  public static <T> Set<Class<? extends T>> scanForImplementations(Class<T> baseClass, final List<String> scanPackages) {
    synchronized(SYNC) {
      Set<Class<? extends T>> classes = getReflections().getSubTypesOf(baseClass);
      for (Iterator<Class<? extends T>> i = classes.iterator(); i.hasNext();) {
        Class<? extends T> c = i.next();
        assert baseClass.isAssignableFrom(c);
        if (Modifier.isAbstract(c.getModifiers())) {
          i.remove();
        }
      }
      return classes;
    }
  }

  private static Collection<URL> getMarkedPaths() {
    Collection<URL> urls = forResource(CommonConstants.DRILL_JAR_MARKER_FILE, true);
    return urls;
  }

  public static Collection<URL> getConfigURLs() {
    return forResource(CommonConstants.DRILL_JAR_MARKER_FILE, false);
  }

  public static Set<URL> forResource(String name, boolean stripName, ClassLoader... classLoaders) {
    final Set<URL> result = Sets.newHashSet();

    final ClassLoader[] loaders = ClasspathHelper.classLoaders(classLoaders);
    final String resourceName = name;

    for (ClassLoader classLoader : loaders) {
        try {
            final Enumeration<URL> urls = classLoader.getResources(resourceName);
            while (urls.hasMoreElements()) {
                final URL url = urls.nextElement();

                int index = url.toExternalForm().lastIndexOf(resourceName);
                if (index != -1 && stripName) {
                    result.add(new URL(url.toExternalForm().substring(0, index)));
                } else {
                    result.add(url);
                }
            }
        } catch (IOException e) {
            if (Reflections.log != null) {
                Reflections.log.error("error getting resources for package " + name, e);
            }
        }
    }

    return result;
  }

}
