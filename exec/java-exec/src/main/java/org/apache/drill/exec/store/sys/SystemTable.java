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
import org.apache.drill.exec.server.options.OptionValue.Kind;
import org.apache.drill.exec.server.options.OptionValue.OptionType;
import org.apache.drill.exec.store.sys.SystemTable.OptionValueWrapper.Status;

import java.util.Iterator;

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
      final DrillConfigIterator configOptions = new DrillConfigIterator(context.getConfig());
      final OptionManager fragmentOptions = context.getOptions();
      final Iterator<OptionValue> mergedOptions = Iterators.concat(configOptions.iterator(), fragmentOptions.iterator());
      final Iterator<OptionValueWrapper> optionValues = new Iterator<OptionValueWrapper>() {
        @Override
        public boolean hasNext() {
          return mergedOptions.hasNext();
        }

        @Override
        public OptionValueWrapper next() {
          final OptionValue value = mergedOptions.next();
          final Status status;
          if (value.type == OptionType.BOOT) {
            status = Status.BOOT;
          } else {
            final OptionValue def = fragmentOptions.getSystemManager().getDefault(value.name);
            if (value.equals(def)) {
              status = Status.DEFAULT;
            } else {
              status = Status.CHANGED;
            }
          }
          return new OptionValueWrapper(value.name, value.kind, value.type, value.num_val, value.string_val,
            value.bool_val, value.float_val, status);
        }

        @Override
        public void remove() {
        }
      };
      @SuppressWarnings("unchecked")
      final Iterator<Object> iterator = (Iterator<Object>) (Object) optionValues;
      return iterator;
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

  public Class getPojoClass() {
    return pojoClass;
  }

  /**
   * Wrapper class for OptionValue to add Status
   */
  public static class OptionValueWrapper {

    public static enum Status {
      BOOT, DEFAULT, CHANGED
    }

    public final String name;
    public final Kind kind;
    public final OptionType type;
    public final Status status;
    public final Long num_val;
    public final String string_val;
    public final Boolean bool_val;
    public final Double float_val;

    public OptionValueWrapper(final String name, final Kind kind, final OptionType type, final Long num_val,
                              final String string_val, final Boolean bool_val, final Double float_val,
                              final Status status) {
      this.name = name;
      this.kind = kind;
      this.type = type;
      this.num_val = num_val;
      this.string_val = string_val;
      this.bool_val = bool_val;
      this.float_val = float_val;
      this.status = status;
    }
  }
}
