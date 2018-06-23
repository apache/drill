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
import org.apache.drill.exec.store.hive.HiveSubScan;
import org.apache.drill.exec.store.hive.readers.HiveAbstractReader;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

/**
 * If table is empty creates an empty record reader to output the schema.
 */
public class EmptyReadersInitializer extends AbstractReadersInitializer {

  public EmptyReadersInitializer(FragmentContext context, HiveSubScan config, Class<? extends HiveAbstractReader> readerClass) {
    super(context, config, readerClass);
  }

  @Override
  public List<RecordReader> init() {
    List<RecordReader> readers = new ArrayList<>(1);
    Constructor<? extends HiveAbstractReader> readerConstructor = createReaderConstructor();
    readers.add(createReader(readerConstructor, null, null));
    return readers;
  }

}
