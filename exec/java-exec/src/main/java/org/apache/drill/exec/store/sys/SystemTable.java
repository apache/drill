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

import java.util.Iterator;

import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.store.sys.OptionIterator.OptionValueWrapper;
import org.apache.drill.exec.store.sys.ExtendedOptionIterator.ExtendedOptionValueWrapper;

/**
 * An enumeration of all tables in Drill's system ("sys") schema.
 * <p>
 *   OPTION, DRILLBITS and VERSION are local tables available on every Drillbit.
 *   MEMORY and THREADS are distributed tables with one record on every
 *   Drillbit.
 * </p>
 */
public enum SystemTable {

  OPTION("options", false, OptionValueWrapper.class) {
    @Override
    public Iterator<Object> getIterator(final FragmentContext context) {
      return new OptionIterator(context, OptionIterator.Mode.SYS_SESS);
    }
  },

  OPTION2("options2", false,ExtendedOptionIterator.ExtendedOptionValueWrapper.class ) {
    @Override
    public Iterator<Object> getIterator(final FragmentContext context) {
      return new ExtendedOptionIterator(context);
    }
  },

  BOOT("boot", false, OptionValueWrapper.class) {
    @Override
    public Iterator<Object> getIterator(final FragmentContext context) {
      return new OptionIterator(context, OptionIterator.Mode.BOOT);
    }
  },

  DRILLBITS("drillbits", false,DrillbitIterator.DrillbitInstance.class) {
    @Override
    public Iterator<Object> getIterator(final FragmentContext context) {
      return new DrillbitIterator(context);
    }
  },

  VERSION("version", false, VersionIterator.VersionInfo.class) {
    @Override
    public Iterator<Object> getIterator(final FragmentContext context) {
      return new VersionIterator();
    }
  },

  MEMORY("memory", true, MemoryIterator.MemoryInfo.class) {
    @Override
    public Iterator<Object> getIterator(final FragmentContext context) {
      return new MemoryIterator(context);
    }
  },

  THREADS("threads", true, ThreadsIterator.ThreadsInfo.class) {
    @Override
  public Iterator<Object> getIterator(final FragmentContext context) {
      return new ThreadsIterator(context);
    }
  };

//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SystemTable.class);

  private final String tableName;
  private final boolean distributed;
  private final Class<?> pojoClass;

  SystemTable(final String tableName, final boolean distributed, final Class<?> pojoClass) {
    this.tableName = tableName;
    this.distributed = distributed;
    this.pojoClass = pojoClass;
  }

  public Iterator<Object> getIterator(final FragmentContext context) {
    throw new UnsupportedOperationException(tableName + " must override this method.");
  }

  public String getTableName() {
    return tableName;
  }

  public boolean isDistributed() {
    return distributed;
  }

  public Class<?> getPojoClass() {
    return pojoClass;
  }


}
