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

package org.apache.drill.exec.resolver;

import java.util.HashMap;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;

public class ResolverTypePrecedence {
	

public static final Map<MinorType, Integer> precedenceMap;

  static {    
    /* The precedenceMap is used to decide whether it's allowed to implicitly "promote" 
     * one type to another type. 
     * 
     * The order that each type is inserted into HASHMAP decides its precedence. 
     * First in ==> lowest precedence. 
     * A type of lower precedence can be implicitly "promoted" to type of higher precedence.
     * For instance, NULL could be promoted to any other type; 
     * tinyint could be promoted into int; but int could NOT be promoted into tinyint (due to possible precision loss).  
     */
    int i = 0;
    precedenceMap = new HashMap<MinorType, Integer>();
   	precedenceMap.put(MinorType.NULL, i += 2);       // NULL is legal to implicitly be promoted to any other type
  	precedenceMap.put(MinorType.FIXEDBINARY, i += 2); // Fixed-length is promoted to var length
  	precedenceMap.put(MinorType.VARBINARY, i += 2);
    precedenceMap.put(MinorType.FIXEDCHAR, i += 2);
   	precedenceMap.put(MinorType.VARCHAR, i += 2);
    precedenceMap.put(MinorType.FIXED16CHAR, i += 2);
   	precedenceMap.put(MinorType.VAR16CHAR, i += 2);
   	precedenceMap.put(MinorType.BIT, i += 2);
   	precedenceMap.put(MinorType.TINYINT, i += 2);   //type with few bytes is promoted to type with more bytes ==> no data loss.
   	precedenceMap.put(MinorType.UINT1, i += 2);     //signed is legal to implicitly be promoted to unsigned.
   	precedenceMap.put(MinorType.SMALLINT, i += 2);
   	precedenceMap.put(MinorType.UINT2, i += 2);
  	precedenceMap.put(MinorType.INT, i += 2);
  	precedenceMap.put(MinorType.UINT4, i += 2);
  	precedenceMap.put(MinorType.BIGINT, i += 2);
  	precedenceMap.put(MinorType.UINT8, i += 2);
  	precedenceMap.put(MinorType.MONEY, i += 2);
  	precedenceMap.put(MinorType.DECIMAL4, i += 2);
  	precedenceMap.put(MinorType.DECIMAL8, i += 2);
  	precedenceMap.put(MinorType.DECIMAL12, i += 2);
  	precedenceMap.put(MinorType.DECIMAL16, i += 2);
  	precedenceMap.put(MinorType.FLOAT4, i += 2);
  	precedenceMap.put(MinorType.FLOAT8, i += 2);
  	precedenceMap.put(MinorType.TIME, i += 2);
  	precedenceMap.put(MinorType.DATE, i += 2);
  	precedenceMap.put(MinorType.DATETIME, i += 2);
    precedenceMap.put(MinorType.TIMETZ, i += 2);
    precedenceMap.put(MinorType.TIMESTAMP, i += 2);
    
  }

}
