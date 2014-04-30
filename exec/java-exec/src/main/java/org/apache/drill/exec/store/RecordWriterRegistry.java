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
package org.apache.drill.exec.store;

import com.google.common.collect.Maps;
import org.apache.drill.common.util.PathScanner;
import org.apache.drill.exec.store.writer.RecordWriterTemplate;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;

public class RecordWriterRegistry {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecordWriterRegistry.class);

  // Contains mapping of "format" to RecordWriter implementation class.
  private static Map<String, Class<? extends RecordWriter>> formatRegistry;

  // Contains mapping of RecordWriter class to standard constructor that is used to instantiate the RecordWriter object.
  private static Map<Class<? extends RecordWriter>, Constructor<? extends RecordWriter>> constructorMap;

  static {
    formatRegistry = Maps.newHashMap();
    constructorMap = Maps.newHashMap();

    Class<?>[] rwc = PathScanner.scanForImplementationsArr(RecordWriter.class, null);

    for(Class<?> clazz : rwc) {
      RecordWriterTemplate template = clazz.getAnnotation(RecordWriterTemplate.class);
      if(template == null){
        logger.warn("{} doesn't have {} annotation. Skipping.", clazz.getCanonicalName(), RecordWriterTemplate.class);
        continue;
      }

      if (template.format() == null || template.format().isEmpty()) {
        logger.warn("{} annotation doesn't have valid format field. Skipping.", RecordWriterTemplate.class);
        continue;
      }

      // Find the standard empty parameter constructor and store it in map.
      Constructor<?> validConstructor = null;
      for(Constructor<?> c : clazz.getConstructors()) {
        if (c.getParameterTypes().length == 0) {
          validConstructor = c;
          break;
        }
      }

      if (validConstructor != null) {
        formatRegistry.put(template.format(), (Class<? extends RecordWriter>)clazz);
        constructorMap.put((Class<? extends RecordWriter>)clazz, (Constructor<? extends RecordWriter>)validConstructor);
      } else {
        logger.info("Skipping RecordWriter class '{}' since it doesn't implement a constructor [{}()]",
            clazz.getCanonicalName(), clazz.getName());
      }
    }
  }

  public static RecordWriter get(String format, Map<String, String> options) throws IOException {

    if (formatRegistry.containsKey(format)) {
      try {
        RecordWriter writer = constructorMap.get(formatRegistry.get(format)).newInstance();
        writer.init(options);
        return writer;
      } catch(Exception e) {
        logger.debug("Failed to create RecordWriter. Received format: {}, options: {}", format, options, e);
        throw new IOException(
            String.format("Failed to create RecordWriter for format '%s' with options '%s'", format, options), e);
      }
    }

    logger.error("Unknown format '{}' received", format);
    throw new IOException(String.format("Unknown format '%s' received", format));
  }
}