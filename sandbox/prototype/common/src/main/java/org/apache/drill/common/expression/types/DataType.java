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
package org.apache.drill.common.expression.types;

public abstract class DataType {
  
  public static enum Comparability{
    UNKNOWN, NONE, EQUAL, ORDERED;
  }
  
  public abstract String getName();
  public abstract boolean isLateBind();
  public abstract boolean hasChildType();
  public abstract DataType getChildType();
  public abstract Comparability getComparability();
  public abstract boolean isNumericType();
  
  
  public static final DataType LATEBIND = new LateBindType();
  public static final DataType BOOLEAN = new AtomType("boolean", Comparability.EQUAL, false);
  public static final DataType BYTES = new AtomType("bytes", Comparability.ORDERED, false);
  public static final DataType NVARCHAR = new AtomType("varchar", Comparability.ORDERED, false);
  public static final DataType FLOAT32 = new AtomType("float32", Comparability.ORDERED, true);
  public static final DataType FLOAT64 = new AtomType("float64", Comparability.ORDERED, true);
  public static final DataType INT64 = new AtomType("int64", Comparability.ORDERED, true);
  public static final DataType INT32 = new AtomType("int32", Comparability.ORDERED, true);
//  public static final DataType INT16 = new AtomType("int16", Comparability.ORDERED, true);
//  public static final DataType BIG_INTEGER = new AtomType("bigint", Comparability.ORDERED, true);
//  public static final DataType BIG_DECIMAL = new AtomType("bigdecimal", Comparability.ORDERED, true);
  public static final DataType DATE = new AtomType("date", Comparability.ORDERED, false);
  public static final DataType DATETIME = new AtomType("datetime", Comparability.ORDERED, false);
  public static final DataType MAP = new AtomType("map", Comparability.NONE, false);
  public static final DataType ARRAY = new AtomType("array", Comparability.NONE, false);
  public static final DataType NULL = new AtomType("null", Comparability.NONE, false);
}
