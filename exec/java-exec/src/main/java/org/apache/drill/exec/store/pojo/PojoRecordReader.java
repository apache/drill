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
import java.sql.Timestamp;
import java.util.Iterator;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.pojo.Writers.BitWriter;
import org.apache.drill.exec.store.pojo.Writers.DoubleWriter;
import org.apache.drill.exec.store.pojo.Writers.EnumWriter;
import org.apache.drill.exec.store.pojo.Writers.IntWriter;
import org.apache.drill.exec.store.pojo.Writers.LongWriter;
import org.apache.drill.exec.store.pojo.Writers.NBigIntWriter;
import org.apache.drill.exec.store.pojo.Writers.NBooleanWriter;
import org.apache.drill.exec.store.pojo.Writers.NDoubleWriter;
import org.apache.drill.exec.store.pojo.Writers.NIntWriter;
import org.apache.drill.exec.store.pojo.Writers.StringWriter;
import org.apache.drill.exec.store.pojo.Writers.NTimeStampWriter;

public class PojoRecordReader<T> implements RecordReader{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PojoRecordReader.class);

  public final int forJsonIgnore = 1;

  private final Class<T> pojoClass;
  private final Iterator<T> iterator;
  private PojoWriter[] writers;
  private boolean doCurrent;
  private T currentPojo;

  public PojoRecordReader(Class<T> pojoClass, Iterator<T> iterator){
    this.pojoClass = pojoClass;
    this.iterator = iterator;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try{
      Field[] fields = pojoClass.getDeclaredFields();
      writers = new PojoWriter[fields.length];
      for(int i = 0; i < writers.length; i++){
        Field f = fields[i];
        Class<?> type = f.getType();

        if(type == int.class){
          writers[i] = new IntWriter(f);
        }else if(type == Integer.class){
          writers[i] = new NIntWriter(f);
        }else if(type == Long.class){
          writers[i] = new NBigIntWriter(f);
        }else if(type == Boolean.class){
          writers[i] = new NBooleanWriter(f);
        }else if(type == double.class){
          writers[i] = new DoubleWriter(f);
        }else if(type == Double.class){
          writers[i] = new NDoubleWriter(f);
        }else if(type.isEnum()){
          writers[i] = new EnumWriter(f);
        }else if(type == boolean.class){
          writers[i] = new BitWriter(f);
        }else if(type == long.class){
          writers[i] = new LongWriter(f);
        }else if(type == String.class){
          writers[i] = new StringWriter(f);
        }else if (type == Timestamp.class) {
          writers[i] = new NTimeStampWriter(f);
        }else{
          throw new ExecutionSetupException(String.format("PojoRecord reader doesn't yet support conversions from type [%s].", type));
        }
        writers[i].init(output);
      }
    }catch(SchemaChangeException e){
      throw new ExecutionSetupException("Failure while setting up schema for PojoRecordReader.", e);
    }

  }

  private void allocate(){
    for(PojoWriter writer : writers){
      writer.allocate();
    }
  }

  private void setValueCount(int i){
    for(PojoWriter writer : writers){
      writer.setValueCount(i);
    }
  }

  @Override
  public int next() {
    boolean allocated = false;

    try{
      int i =0;
      outside:
      while(doCurrent || iterator.hasNext()){
        if(doCurrent){
          doCurrent = false;
        }else{
          currentPojo = iterator.next();
        }

        if(!allocated){
          allocate();
          allocated = true;
        }

        for(PojoWriter writer : writers){
          if(!writer.writeField(currentPojo, i)){
            doCurrent = true;
            if(i == 0) throw new IllegalStateException("Got into a position where we can't write data but the batch is empty.");
            break outside;
          };
        }
        i++;
      }

      if(i != 0 ) setValueCount(i);
      return i;
    } catch (IllegalArgumentException | IllegalAccessException e) {
      throw new RuntimeException("Failure while trying to use PojoRecordReader.", e);
    }
  }

  @Override
  public void cleanup() {
  }


}
