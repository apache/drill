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
package org.apache.drill.exec.store.hive.readers.initilializers;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.store.hive.HiveSubScan;
import org.apache.drill.exec.store.hive.readers.HiveAbstractReader;
import org.apache.drill.exec.store.hive.readers.HiveAvroReader;
import org.apache.drill.exec.store.hive.readers.HiveDefaultReader;
import org.apache.drill.exec.store.hive.readers.HiveOrcReader;
import org.apache.drill.exec.store.hive.readers.HiveParquetReader;
import org.apache.drill.exec.store.hive.readers.HiveRCFileReader;
import org.apache.drill.exec.store.hive.readers.HiveTextReader;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import java.util.HashMap;
import java.util.Map;

public class ReadersInitializer {

  /**
   * List of all available readers classes for a different Hive nativ formats:
   * ORC, AVRO, RCFFile, Text and Parquet.
   */
  private static final Map<String, Class<? extends HiveAbstractReader>> READER_MAP = new HashMap<>();

  static {
    READER_MAP.put(OrcInputFormat.class.getCanonicalName(), HiveOrcReader.class);
    READER_MAP.put(AvroContainerInputFormat.class.getCanonicalName(), HiveAvroReader.class);
    READER_MAP.put(RCFileInputFormat.class.getCanonicalName(), HiveRCFileReader.class);
    READER_MAP.put(MapredParquetInputFormat.class.getCanonicalName(), HiveParquetReader.class);
    READER_MAP.put(TextInputFormat.class.getCanonicalName(), HiveTextReader.class);
  }

  /**
   * Determines which reader initializer should be used got given table configuration.
   * Decision is made based on table content and skip header / footer logic usage.
   *
   * @param context fragment context
   * @param config Hive table config
   * @return reader initializer
   */
  public static AbstractReadersInitializer getInitializer(FragmentContext context, HiveSubScan config) {
    Class<? extends HiveAbstractReader> readerClass = getReaderClass(config);
    if (config.getInputSplits().isEmpty()) {
      return new EmptyReadersInitializer(context, config, readerClass);
    } else {
      return new DefaultReadersInitializer(context, config, readerClass);
    }
  }

  /**
   * Will try to find reader class based on Hive table input format.
   * If reader class was not find, will use default reader class.
   *
   * @param config Hive table config
   * @return reader class
   */
  private static Class<? extends HiveAbstractReader> getReaderClass(HiveSubScan config) {
    final String formatName = config.getTable().getSd().getInputFormat();
    Class<? extends HiveAbstractReader> readerClass = HiveDefaultReader.class;
    if (READER_MAP.containsKey(formatName)) {
      readerClass = READER_MAP.get(formatName);
    }
    return readerClass;
  }

}
