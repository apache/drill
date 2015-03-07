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
package org.apache.drill.exec.store.sys;

import com.google.common.collect.Iterators;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.server.options.DrillConfigIterator;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.store.RecordDataType;
import org.apache.drill.exec.store.pojo.PojoDataType;

import java.util.Iterator;

/**
 * An enumeration of all system tables that Drill supports.
 * <p/>
 * OPTION, DRILLBITS and VERSION are local tables available on every Drillbit.
 * MEMORY and THREADS are distributed tables with one record on every Drillbit.
 */
public enum SystemTable {

  OPTION("options", false, new PojoDataType(OptionValue.class)) {
    @Override
    public Iterator<Object> getLocalIterator(final FragmentContext context) {
      final DrillConfigIterator configOptions = new DrillConfigIterator(context.getConfig());
      final OptionManager fragmentOptions = context.getOptions();
      return (Iterator<Object>) (Object) Iterators.concat(configOptions.iterator(), fragmentOptions.iterator());
    }
  },

  DRILLBITS("drillbits", false, new PojoDataType(DrillbitIterator.DrillbitInstance.class)) {
    @Override
    public Iterator<Object> getLocalIterator(final FragmentContext context) {
      return new DrillbitIterator(context);
    }
  },

  VERSION("version", false, new PojoDataType(VersionIterator.VersionInfo.class)) {
    @Override
    public Iterator<Object> getLocalIterator(final FragmentContext context) {
      return new VersionIterator();
    }
  },

  MEMORY("memory", true, MemoryRecord.getInstance()) {
    @Override
    public SystemRecord getSystemRecord() {
      return MemoryRecord.getInstance();
    }
  },

  THREADS("threads", true, ThreadsRecord.getInstance()) {
    @Override
    public SystemRecord getSystemRecord() {
      return ThreadsRecord.getInstance();
    }
  };

//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemTable.class);

  private final String tableName;
  private final boolean distributed;
  private final RecordDataType dataType;

  SystemTable(String tableName, boolean distributed, RecordDataType dataType) {
    this.tableName = tableName;
    this.distributed = distributed;
    this.dataType = dataType;
  }

  // Distributed tables must override this method
  public SystemRecord getSystemRecord() {
    throw new UnsupportedOperationException("Local table does not support this function.");
  }

  // Local tables must override this method
  public Iterator<Object> getLocalIterator(FragmentContext context) {
    throw new UnsupportedOperationException("Distributed table does not support this function.");
  }

  public String getTableName() {
    return tableName;
  }

  public boolean isDistributed() {
    return distributed;
  }

  public RecordDataType getDataType() {
    return dataType;
  }

}
