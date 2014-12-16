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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;
import org.apache.drill.exec.util.DecimalUtility;

public class TypeCastRules {

  private static Map<MinorType, Set<MinorType>> rules;

  public TypeCastRules() {
  }

  static {
    initTypeRules();
  }

  private static void initTypeRules() {
    rules = new HashMap<MinorType, Set<MinorType>>();

    Set<MinorType> rule;

    /** TINYINT cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.TINYINT, rule);

    /** SMALLINT cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.SMALLINT, rule);

    /** INT cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.INT, rule);

    /** BIGINT cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.BIGINT, rule);

    /** UINT8 cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.UINT8, rule);

    /** DECIMAL9 cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.DECIMAL9, rule);

    /** DECIMAL18 cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.DECIMAL18, rule);

    /** DECIMAL28Dense cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.DECIMAL28DENSE, rule);

    /** DECIMAL28Sparse cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.DECIMAL28SPARSE, rule);

    /** DECIMAL38Dense cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.DECIMAL38DENSE, rule);


    /** DECIMAL38Sparse cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.DECIMAL38SPARSE, rule);

    /** MONEY cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.MONEY, rule);

    /** DATE cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.DATE, rule);

    /** TIME cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TIME);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.TIME, rule);

    /** TIMESTAMP cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIME);
    rule.add(MinorType.TIMESTAMPTZ);
    rules.put(MinorType.TIMESTAMP, rule);

    /** TIMESTAMPTZ cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.TIME);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.TIMESTAMPTZ, rule);

    /** Interval cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.INTERVAL);
    rule.add(MinorType.INTERVALDAY);
    rule.add(MinorType.INTERVALYEAR);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.INTERVAL, rule);

    /** INTERVAL YEAR cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.INTERVALYEAR);
    rule.add(MinorType.INTERVAL);
    rule.add(MinorType.INTERVALDAY);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.INTERVALYEAR, rule);

    /** INTERVAL DAY cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.INTERVALDAY);
    rule.add(MinorType.INTERVALYEAR);
    rule.add(MinorType.INTERVAL);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.INTERVALDAY, rule);

    /** FLOAT4 cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rules.put(MinorType.FLOAT4, rule);

    /** FLOAT8 cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rules.put(MinorType.FLOAT8, rule);

    /** BIT cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rules.put(MinorType.BIT, rule);

    /** FIXEDCHAR cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIME);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.INTERVAL);
    rule.add(MinorType.INTERVALYEAR);
    rule.add(MinorType.INTERVALDAY);
    rules.put(MinorType.FIXEDCHAR, rule);

    /** FIXED16CHAR cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIME);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.INTERVAL);
    rule.add(MinorType.INTERVALYEAR);
    rule.add(MinorType.INTERVALDAY);
    rules.put(MinorType.FIXED16CHAR, rule);

    /** FIXEDBINARY cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rules.put(MinorType.FIXEDBINARY, rule);

    /** VARCHAR cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIME);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.INTERVAL);
    rule.add(MinorType.INTERVALYEAR);
    rule.add(MinorType.INTERVALDAY);
    rules.put(MinorType.VARCHAR, rule);

    /** VAR16CHAR cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rule.add(MinorType.DATE);
    rule.add(MinorType.TIME);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.INTERVAL);
    rule.add(MinorType.INTERVALYEAR);
    rule.add(MinorType.INTERVALDAY);
    rules.put(MinorType.VAR16CHAR, rule);

    /** VARBINARY cast able from **/
    rule = new HashSet<MinorType>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.TIMESTAMP);
    rule.add(MinorType.TIMESTAMPTZ);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rules.put(MinorType.VARBINARY, rule);
  }

  public static boolean isCastableWithNullHandling(MajorType from, MajorType to, NullHandling nullHandling) {
    if ((from.getMode() == DataMode.REPEATED || to.getMode() == DataMode.REPEATED) && from.getMode() != to.getMode()) {
      return false;
    }
    if (nullHandling == NullHandling.INTERNAL && from.getMode() != to.getMode()) {
      return false;
    }
    return isCastable(from.getMinorType(), to.getMinorType());
  }

  private static boolean isCastable(MinorType from, MinorType to) {
    return from.equals(MinorType.NULL) ||      //null could be casted to any other type.
        (rules.get(to) == null ? false : rules.get(to).contains(from));
  }

  /*
   * Function checks if casting is allowed from the 'from' -> 'to' minor type. If its allowed
   * we also check if the precedence map allows such a cast and return true if both cases are satisfied
   */
  public static MinorType getLeastRestrictiveType(List<MinorType> types) {
    assert types.size() >= 2;
    MinorType result = types.get(0);
    int resultPrec = ResolverTypePrecedence.precedenceMap.get(result);

    for (int i = 1; i < types.size(); i++) {
      MinorType next = types.get(i);
      if (next == result) {
        // both args are of the same type; continue
        continue;
      }

      int nextPrec = ResolverTypePrecedence.precedenceMap.get(next);

      if (isCastable(next, result) && resultPrec >= nextPrec) {
        // result is the least restrictive between the two args; nothing to do continue
        continue;
      } else if(isCastable(result, next) && nextPrec >= resultPrec) {
        result = next;
        resultPrec = nextPrec;
      } else {
        return null;
      }
    }

    return result;
  }

  private static final int DATAMODE_CAST_COST = 1;

  /*
   * code decide whether it's legal to do implicit cast. -1 : not allowed for
   * implicit cast > 0: cost associated with implicit cast. ==0: parms are
   * exactly same type of arg. No need of implicit.
   */
  public static int getCost(FunctionCall call, DrillFuncHolder holder) {
    int cost = 0;

    if (call.args.size() != holder.getParamCount()) {
      return -1;
    }

    // Indicates whether we used secondary cast rules
    boolean secondaryCast = false;

    // number of arguments that could implicitly casts using precedence map or didn't require casting at all
    int nCasts = 0;

    /*
     * If we are determining function holder for decimal data type, we need to make sure the output type of
     * the function can fit the precision that we need based on the input types.
     */
    if (holder.checkPrecisionRange() == true) {
      if (DecimalUtility.getMaxPrecision(holder.getReturnType().getMinorType()) < holder.getReturnType(call.args).getPrecision()) {
        return -1;
      }
    }

    for (int i = 0; i < holder.getParamCount(); i++) {
      MajorType argType = call.args.get(i).getMajorType();
      MajorType parmType = holder.getParmMajorType(i);

      //@Param FieldReader will match any type
      if (holder.isFieldReader(i)) {
//        if (Types.isComplex(call.args.get(i).getMajorType()) ||Types.isRepeated(call.args.get(i).getMajorType()) )
          continue;
//        else
//          return -1;
      }

      if (!TypeCastRules.isCastableWithNullHandling(argType, parmType, holder.getNullHandling())) {
        return -1;
      }

      Integer parmVal = ResolverTypePrecedence.precedenceMap.get(parmType
          .getMinorType());
      Integer argVal = ResolverTypePrecedence.precedenceMap.get(argType
          .getMinorType());

      if (parmVal == null) {
        throw new RuntimeException(String.format(
            "Precedence for type %s is not defined", parmType.getMinorType()
                .name()));
      }

      if (argVal == null) {
        throw new RuntimeException(String.format(
            "Precedence for type %s is not defined", argType.getMinorType()
                .name()));
      }

      if (parmVal - argVal < 0) {

        /* Precedence rules does not allow to implicitly cast, however check
         * if the seconday rules allow us to cast
         */
        Set<MinorType> rules;
        if ((rules = (ResolverTypePrecedence.secondaryImplicitCastRules.get(parmType.getMinorType()))) != null &&
            rules.contains(argType.getMinorType()) != false) {
          secondaryCast = true;
        } else {
          return -1;
        }
      }
      // Check null vs non-null, using same logic as that in Types.softEqual()
      // Only when the function uses NULL_IF_NULL, nullable and non-nullable are inter-changable.
      // Otherwise, the function implementation is not a match.
      if (argType.getMode() != parmType.getMode()) {
        // TODO - this does not seem to do what it is intended to
//        if (!((holder.getNullHandling() == NullHandling.NULL_IF_NULL) &&
//            (argType.getMode() == DataMode.OPTIONAL ||
//             argType.getMode() == DataMode.REQUIRED ||
//             parmType.getMode() == DataMode.OPTIONAL ||
//             parmType.getMode() == DataMode.REQUIRED )))
//          return -1;
        // if the function is designed to take optional with custom null handling, and a required
        // is being passed, increase the cost to account for a null check
        // this allows for a non-nullable implementation to be preferred
        if (holder.getNullHandling() == NullHandling.INTERNAL) {
          // a function that expects required output, but nullable was provided
          if (parmType.getMode() == DataMode.REQUIRED && argType.getMode() == DataMode.OPTIONAL) {
            return -1;
          }
          else if (parmType.getMode() == DataMode.OPTIONAL && argType.getMode() == DataMode.REQUIRED) {
            cost+= DATAMODE_CAST_COST;
          }
        }
      }

      int castCost;

      if ((castCost = (parmVal - argVal)) >= 0) {
        nCasts++;
        cost += castCost;
      }
    }

    if (secondaryCast) {
      // We have a secondary cast for one or more of the arguments, determine the cost associated
      int secondaryCastCost =  Integer.MAX_VALUE - 1;

      // Subtract maximum possible implicit costs from the secondary cast cost
      secondaryCastCost -= (nCasts * (ResolverTypePrecedence.MAX_IMPLICIT_CAST_COST + DATAMODE_CAST_COST));

      // Add cost of implicitly casting the rest of the arguments that didn't use secondary casting
      secondaryCastCost += cost;

      return secondaryCastCost;
    }

    return cost;
  }

}
