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
package org.apache.drill.exec.store.pojo;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField.Key;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.pojo.Writers.BitWriter;
import org.apache.drill.exec.store.pojo.Writers.DoubleWriter;
import org.apache.drill.exec.store.pojo.Writers.EnumWriter;
import org.apache.drill.exec.store.pojo.Writers.IntWriter;
import org.apache.drill.exec.store.pojo.Writers.LongWriter;
import org.apache.drill.exec.store.pojo.Writers.NBigIntWriter;
import org.apache.drill.exec.store.pojo.Writers.NBooleanWriter;
import org.apache.drill.exec.store.pojo.Writers.NDoubleWriter;
import org.apache.drill.exec.store.pojo.Writers.NIntWriter;
import org.apache.drill.exec.store.pojo.Writers.NTimeStampWriter;
import org.apache.drill.exec.store.pojo.Writers.StringWriter;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.collect.Lists;

public class PojoRecordReader<T> extends AbstractRecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PojoRecordReader.class);

  public final int forJsonIgnore = 1;

  private final Class<T> pojoClass;
  private final Iterator<T> iterator;
  private PojoWriter[] writers;
  private boolean doCurrent;
  private T currentPojo;
  private OperatorContext operatorContext;

  public PojoRecordReader(Class<T> pojoClass, Iterator<T> iterator) {
    this.pojoClass = pojoClass;
    this.iterator = iterator;
  }

  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  public void setOperatorContext(OperatorContext operatorContext) {
    this.operatorContext = operatorContext;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      Field[] fields = pojoClass.getDeclaredFields();
      List<PojoWriter> writers = Lists.newArrayList();

      for (int i = 0; i < fields.length; i++) {
        Field f = fields[i];

        if (Modifier.isStatic(f.getModifiers())) {
          continue;
        }

        Class<?> type = f.getType();
        PojoWriter w = null;
        if(type == int.class) {
          w = new IntWriter(f);
        } else if(type == Integer.class) {
          w = new NIntWriter(f);
        } else if(type == Long.class) {
          w = new NBigIntWriter(f);
        } else if(type == Boolean.class) {
          w = new NBooleanWriter(f);
        } else if(type == double.class) {
          w = new DoubleWriter(f);
        } else if(type == Double.class) {
          w = new NDoubleWriter(f);
        } else if(type.isEnum()) {
          w = new EnumWriter(f, output.getManagedBuffer());
        } else if(type == boolean.class) {
          w = new BitWriter(f);
        } else if(type == long.class) {
          w = new LongWriter(f);
        } else if(type == String.class) {
          w = new StringWriter(f, output.getManagedBuffer());
        } else if (type == Timestamp.class) {
          w = new NTimeStampWriter(f);
        } else {
          throw new ExecutionSetupException(String.format("PojoRecord reader doesn't yet support conversions from type [%s].", type));
        }
        writers.add(w);
        w.init(output);
      }

      this.writers = writers.toArray(new PojoWriter[writers.size()]);

    } catch(SchemaChangeException e) {
      throw new ExecutionSetupException("Failure while setting up schema for PojoRecordReader.", e);
    }


  }

  @Override
  public void allocate(Map<Key, ValueVector> vectorMap) throws OutOfMemoryException {
    for (ValueVector v : vectorMap.values()) {
      AllocationHelper.allocate(v, Character.MAX_VALUE, 50, 10);
    }
  }

  private void allocate() {
    for (PojoWriter writer : writers) {
      writer.allocate();
    }
  }

  private void setValueCount(int i) {
    for (PojoWriter writer : writers) {
      writer.setValueCount(i);
    }
  }

  @Override
  public int next() {
    boolean allocated = false;

    try {
      int i =0;
      outside:
      while (doCurrent || iterator.hasNext()) {
        if (doCurrent) {
          doCurrent = false;
        } else {
          currentPojo = iterator.next();
        }

        if (!allocated) {
          allocate();
          allocated = true;
        }

        for (PojoWriter writer : writers) {
          writer.writeField(currentPojo, i);
        }
        i++;
      }

      if (i != 0 ) {
        setValueCount(i);
      }
      return i;
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException("Failure while trying to use PojoRecordReader.", e);
    }
  }

  @Override
  public void cleanup() {
  }

}
