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
package org.apache.drill.exec.cache;

import com.hazelcast.nio.DataSerializable;

import java.io.*;

/**
 * Wraps a DrillSerializable object. Objects of this class can be put in the HazelCast implementation of Distributed Cache
 */
public abstract class HCDrillSerializableWrapper implements DataSerializable {

  private DrillSerializable obj;

  public HCDrillSerializableWrapper() {}

  public HCDrillSerializableWrapper(DrillSerializable obj) {
    this.obj = obj;
  }

  public void readData(DataInput in) throws IOException {
    obj.read(in);
  }

  public void writeData(DataOutput out) throws IOException {
    obj.write(out);
  }

  public DrillSerializable get() {
    return obj;
  }

  /**
   *  This is a method that will get a Class specific implementation of HCDrillSerializableWrapper. Class specific implentations
   *  are necessary because Hazel Cast requires object that have constructors with no parameters.
   * @param value
   * @param clazz
   * @return
   */
  public static HCDrillSerializableWrapper getWrapper(DrillSerializable value, Class clazz) {
    if (clazz.equals(VectorContainerSerializable.class)) {
      return new HCSerializableWrapperClasses.HCVectorListSerializable(value);
    } else {
      throw new UnsupportedOperationException("HCDrillSerializableWrapper not implemented for " + clazz);
    }
  }
}
