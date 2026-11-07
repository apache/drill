/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.security.ranger;

import org.apache.ranger.plugin.classloader.RangerPluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

/**
 * A {@link RangerPluginClassLoader} subclass that blocks the Jersey 3.1.9
 * MultiPart SPI from leaking into the Jersey 2.35 Ranger client via the
 * parent (Drillbit) classpath.
 */
public final class DrillRangerPluginClassLoader extends RangerPluginClassLoader {

  private static final Logger logger = LoggerFactory.getLogger(DrillRangerPluginClassLoader.class);

  /**
   * SPI resource name that Jersey's {@code ServiceFinder} scans to locate
   * auto-discoverable providers. Jersey 2.35 and 3.1.9 share this file
   * name (the SPI contract is package-private and unchanged across the
   * two versions).
   */
  private static final String AUTODISCOVERABLE_SPI =
      "META-INF/services/org.glassfish.jersey.internal.spi.AutoDiscoverable";

  /**
   * FQN that exists only in Jersey 3.x. Its presence in an
   * {@code AutoDiscoverable} SPI file is a reliable marker that the file
   * comes from a Jersey 3.1.9 jar on the Drillbit classpath and must be
   * hidden from the 2.35 {@code ServiceFinder}.
   */
  private static final String JERSEY3_MULTIPART_MARKER =
      "org.glassfish.jersey.media.multipart.MultiPartFeatureAutodiscoverable";

  public DrillRangerPluginClassLoader(String pluginType, Class<?> pluginClass) throws Exception {
    super(pluginType, pluginClass);
    logger.info("DrillRangerPluginClassLoader initialized for plugin type: {}", pluginType);
  }

  /**
   * Returns merged child+component resources, with the Jersey 3.1.9
   * MultiPart {@code AutoDiscoverable} SPI entry removed when present.
   *
   * <p>Non-SPI resources are returned unchanged so that Ranger's own
   * resource lookups (configuration files, native libraries, etc.) are
   * not affected.</p>
   */
  @Override
  public Enumeration<URL> findResources(String name) {
    // Base RangerPluginClassLoader.findResources does not declare IOException,
    // so super.findResources cannot throw it either; no try/catch needed.
    Enumeration<URL> merged = super.findResources(name);
    if (!AUTODISCOVERABLE_SPI.equals(name)) {
      return merged;
    }
    List<URL> kept = new ArrayList<>();
    while (merged.hasMoreElements()) {
      URL url = merged.nextElement();
      if (!declaresJersey3Multipart(url)) {
        kept.add(url);
      } else {
        logger.debug("Filtered Jersey 3.1.9 MultiPart AutoDiscoverable SPI entry: {}", url);
      }
    }
    return Collections.enumeration(kept);
  }

  /**
   * Returns {@code true} if the given SPI resource URL declares the
   * Jersey 3.x {@code MultiPartFeatureAutodiscoverable} FQN.
   *
   * <p>If the URL cannot be read, conservatively returns {@code false}
   * (keep the entry) to avoid accidentally dropping a legitimate SPI
   * file that might be unreadable due to transient I/O conditions.</p>
   */
  private boolean declaresJersey3Multipart(URL url) {
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(url.openStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        // SPI files use FQNs (optionally with comment/whitespace); a plain
        // contains() check is sufficient and avoids false negatives from
        // edge-case formatting (trailing spaces, trailing comments).
        if (line.contains(JERSEY3_MULTIPART_MARKER)) {
          return true;
        }
      }
    } catch (IOException e) {
      logger.warn("Could not read SPI entry {} to inspect content; keeping it", url, e);
      return false;
    }
    return false;
  }
}
