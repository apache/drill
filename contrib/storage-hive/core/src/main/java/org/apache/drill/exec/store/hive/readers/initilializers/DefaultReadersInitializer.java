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
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.hive.HivePartition;
import org.apache.drill.exec.store.hive.HiveSubScan;
import org.apache.drill.exec.store.hive.readers.HiveAbstractReader;
import org.apache.drill.exec.store.hive.readers.initilializers.AbstractReadersInitializer;
import org.apache.hadoop.mapred.InputSplit;

import java.lang.reflect.Constructor;
import java.util.LinkedList;
import java.util.List;

/**
 * Creates separate record reader for each given input split group.
 */
public class DefaultReadersInitializer extends AbstractReadersInitializer {

  public DefaultReadersInitializer(FragmentContext context, HiveSubScan config, Class<? extends HiveAbstractReader> readerClass) {
    super(context, config, readerClass);
  }

  @Override
  public List<RecordReader> init() {
    List<List<InputSplit>> inputSplits = config.getInputSplits();
    List<HivePartition> partitions = config.getPartitions();
    boolean hasPartitions = partitions != null && !partitions.isEmpty();

    List<RecordReader> readers = new LinkedList<>();
    Constructor<? extends HiveAbstractReader> readerConstructor = createReaderConstructor();
    for (int i = 0; i < inputSplits.size(); i++) {
      readers.add(createReader(readerConstructor, hasPartitions ? partitions.get(i) : null, inputSplits.get(i)));
    }
    return readers;
  }
}
