/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.common.expression;

import java.util.Arrays;

public enum DataType { // roughly taken from sql
  INVALID, BOOLEAN, BYTES, NVARCHAR, INTEGER, SMALLINT, VARBIT, FLOAT, DOUBLE, DATE, TIME, DATETIME, UNKNOWN;

  private DataType[] castable;

  private DataType(DataType... castToDataTypes) {
    castable = castToDataTypes;
    Arrays.sort(castable);
  }

  public boolean canCastTo(DataType dt) {
    if (dt == this)
      return true;
    for (int i = 0; i < castable.length; i++) {
      if (dt.equals(castable[i]))
        return true;
    }
    return false;
  }

  public static DataType getCombinedCast(DataType a, DataType b) {
    if (a == b)
      return a;

    if (a.canCastTo(b))
      return b;
    if (b.canCastTo(a))
      return a;
    return null;
  }

}
