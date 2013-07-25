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
package org.apache.drill.exec.ref.values;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;

public class ValueReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ValueReader.class);
  
  public static boolean getBoolean(DataValue v){
    if(v.getDataType().getMinorType() == MinorType.BOOLEAN && v.getDataType().getMode() != DataMode.REPEATED){
      return v.getAsBooleanValue().getBoolean();
    }else{
      throw new DrillRuntimeException(String.format("Unable to get boolean.  Type was a %s", v.getClass().getCanonicalName()));
    }
  }
  
  public static long getLong(DataValue v){
    if(Types.isNumericType(v.getDataType())){
      return v.getAsNumeric().getAsLong();
    }else{
      throw new DrillRuntimeException(String.format("Unable to get value.  %s is not a numeric type.", v.getClass().getCanonicalName()));
    }
  }
  public static double getDouble(DataValue v){
    if(Types.isNumericType(v.getDataType())){
      return v.getAsNumeric().getAsDouble();
    }else{
      throw new DrillRuntimeException(String.format("Unable to get value.  %s is not a numeric type.", v.getClass().getCanonicalName()));
    }
  }
  public static CharSequence getChars(DataValue v){
    if(Types.isStringScalarType(v.getDataType())){
      return v.getAsStringValue().getString();
    }else{
      throw new DrillRuntimeException(String.format("Unable to get value.  %s is not a StringValue type.", v.getClass().getCanonicalName()));
    }
  }
  
  public static String getString(DataValue v){
    return getChars(v).toString();
  }
  
}
