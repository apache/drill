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
package org.apache.drill.extn;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the extensions registry. Provides extensive logging
 * to help debug the inevitable problems that will occur in the field.
 */
public class ExtensionsRegistryImpl implements ExtensionsRegistry {

  // Attribute logging to the interface for easier debugging.
  private static final Logger logger = LoggerFactory.getLogger(ExtensionsRegistry.class);

  @SuppressWarnings("serial")
  private static class ExtException extends Exception {

    public ExtException(String msg, Exception e) {
      super(msg, e);
    }
  }

  private final File extDir;
  private final Map<Class<?>, ExtensionsRegistryImpl.ServiceProviderRegistryImpl<?>> services = new IdentityHashMap<>();

  public ExtensionsRegistryImpl(File extDir, List<Class<?>> services) {
    this.extDir = extDir;
    defineServices(services);
    loadProviders();
  }

  private void defineServices(List<Class<?>> serviceClasses) {
    for (Class<?> serviceClass : serviceClasses) {
      services.put(serviceClass, new ExtensionsRegistryImpl.ServiceProviderRegistryImpl<>(serviceClass));
      logger.info("Defined extension point: {}", serviceClass.getCanonicalName());
    }
  }

  private void loadProviders() {
    for (File extFile : extDir.listFiles()) {
      if (extFile.isDirectory()) {
        loadExtDir(extFile);
      } else if (extFile.getName().endsWith(".jar")) {
        loadExtJar(null, extFile);
      }
    }
  }

  private void loadExtDir(File providerDir) {
    String jarName = providerDir.getName() + ".jar";
    File providerJar = new File(providerDir, jarName);
    if (providerDir.exists()) {
      loadExtJar(providerDir, providerJar);
    }
  }

  private void loadExtJar(File providerDir, File providerJar) {
    try (JarFile jarFile = new JarFile(providerJar)) {
      for (ExtensionsRegistryImpl.ServiceProviderRegistryImpl<?> serviceReg : services.values()) {
        if (loadService(providerDir, providerJar, jarFile, serviceReg)) {
          return;
        }
      }
      logError(providerDir, providerJar,
          "Did not find a provider config file in META_INF/services/, ignoring jar", null);
    } catch (IOException e) {
      logError(providerDir, providerJar,
          "Could not open extension jar", e);
    } catch (ExtException e) {
      logError(providerDir, providerJar,
          e.getMessage(), e.getCause());
    }
  }

  private void logError(File providerDir, File providerJar, String msg, Throwable e) {
    String path = providerDir == null ? "" : providerDir.getName() + "/";
    path += providerJar.getName();
    logger.error(String.format("%s - Jar: %s", msg, path), e);
  }

  private boolean loadService(File providerDir, File providerJar, JarFile jarFile,
      ExtensionsRegistryImpl.ServiceProviderRegistryImpl<?> serviceReg)
          throws ExtException {
    String implClassName = readMappingFile(jarFile, serviceReg.serviceClass());
    if (implClassName == null) {
      return false;
    }
    ExtensionsRegistryImpl.ProviderHost providerHost = new ProviderHost(providerDir, providerJar, implClassName);
    serviceReg.addProvider(providerHost);
    logger.info("Successfully loaded extension jar {} with class {} implementing {}",
        providerJar.getName(), implClassName,
        serviceReg.serviceClass().getCanonicalName());
    return true;
  }

  private String readMappingFile(JarFile jarFile, Class<?> serviceClass) throws ExtException {
    String className = serviceClass.getCanonicalName();
    String mapFilePath = "META-INF/services/" + className;
    JarEntry jarEntry = jarFile.getJarEntry(mapFilePath);
    if (jarEntry == null) {
      // Should never occur: we got here because the entry exists.
      return null;
    }
    try (BufferedReader br = new BufferedReader(new InputStreamReader(jarFile.getInputStream(jarEntry)))) {
      return br.readLine();
    } catch (IOException e) {
      throw new ExtException(String.format(
          "Failed to read the provider configuration file %s",
          className), e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> ExtensionsRegistry.ServiceProviderRegistry<T> registryFor(Class<T> serviceClass) {
    return (ExtensionsRegistry.ServiceProviderRegistry<T>) services.get(serviceClass);
  }

  public static class ServiceProviderRegistryImpl<T> implements ExtensionsRegistry.ServiceProviderRegistry<T> {

    private final Class<T> serviceClass;
    private final List<ExtensionsRegistryImpl.ProviderHost> providerHosts = new ArrayList<>();
    private final List<T> serviceImpls = new ArrayList<>();

    public ServiceProviderRegistryImpl(Class<T> serviceClass) {
      this.serviceClass = serviceClass;
    }

    protected Class<T> serviceClass() { return serviceClass; }

    protected void addProvider(ExtensionsRegistryImpl.ProviderHost provider) {
      providerHosts.add(provider);
      serviceImpls.add(provider.serviceImpl());
    }

    @Override
    public List<T> providers() {
      return serviceImpls;
    }
  }

  protected static class ProviderHost {

    private final File providerDir;
    private final File providerJar;
    private final ClassLoader classLoader;
    private final Class<?> serviceImplClass;
    private final Object serviceImpl;

    protected ProviderHost(File providerDir, File providerJar, String implClassName)
        throws ExtException {
      this.providerDir = providerDir;
      this.providerJar = providerJar;
      try {
        this.classLoader = buildClassLoader();
      } catch (MalformedURLException e) {
        throw new ExtException(
            "Failed to create a class loader for the extension. Malformed jar names?", e);
      }
      try {
        this.serviceImplClass = classLoader.loadClass(implClassName);
      } catch (ClassNotFoundException e) {
        throw new ExtException(String.format(
            "Extension jar has a provider config file " +
            "in META_INF/services/, but the named class %s not found.",
            implClassName), e);
      }
      try {
        this.serviceImpl = serviceImplClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new ExtException(String.format(
            "Could not create an instance of provider class %s " +
            "using the no-argument constructor", implClassName), e);
      }
    }

    @SuppressWarnings("unchecked")
    public <T> T serviceImpl() {
      return (T) serviceImpl;
    }

    private ClassLoader buildClassLoader() throws MalformedURLException {
      List<URL> jars = new ArrayList<>();
      if (providerDir != null) {
        expandJars(jars);
      } else {
        jars.add(providerJar.toURI().toURL());
      }
      URL[] urls = new URL[jars.size()];
      jars.toArray(urls);

      // TODO: Replace this with a custom, Tomcat-like class loader.
      return new URLClassLoader(urls, getClass().getClassLoader());
    }

    private void expandJars(List<URL> jars) throws MalformedURLException {
      for (File file : providerDir.listFiles()) {
        if (file.getName().endsWith(".jar")) {
          jars.add(file.toURI().toURL());
        }
      }
    }
  }
}
