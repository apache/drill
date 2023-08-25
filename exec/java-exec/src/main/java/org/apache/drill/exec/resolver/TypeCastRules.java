/*
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
import java.util.Optional;
import java.util.Set;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.MajorTypeInLogicalExpression;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.fn.DrillFuncHolder;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import static org.apache.drill.exec.planner.types.DrillRelDataTypeSystem.DRILL_REL_DATATYPE_SYSTEM;

public class TypeCastRules {

  private static Map<MinorType, Set<MinorType>> rules;

  public TypeCastRules() {
  }

  static {
    initTypeRules();
  }

  private static void initTypeRules() {
    rules = new HashMap<>();

    Set<MinorType> rule;

    /** TINYINT cast able from **/
    rule = new HashSet<>();
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
    rule.add(MinorType.VARDECIMAL);
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
    rule = new HashSet<>();
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
    rule.add(MinorType.VARDECIMAL);
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
    rule = new HashSet<>();
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
    rule.add(MinorType.VARDECIMAL);
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
    rule = new HashSet<>();
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
    rule.add(MinorType.VARDECIMAL);
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

    /** UINT4 cast able from **/
    rule = new HashSet<>();
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
    rule.add(MinorType.VARDECIMAL);
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
    rules.put(MinorType.UINT4, rule);

    /** UINT8 cast able from **/
    rule = new HashSet<>();
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
    rule.add(MinorType.VARDECIMAL);
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
    rule = new HashSet<>();
    rule.add(MinorType.DECIMAL9);
    rule.add(MinorType.VARDECIMAL);
    rules.put(MinorType.DECIMAL9, rule);

    /** DECIMAL18 cast able from **/
    rule = new HashSet<>();
    rule.add(MinorType.DECIMAL18);
    rule.add(MinorType.VARDECIMAL);
    rules.put(MinorType.DECIMAL18, rule);

    /** DECIMAL28Dense cast able from **/
    rule = new HashSet<>();
    rule.add(MinorType.DECIMAL28DENSE);
    rule.add(MinorType.VARDECIMAL);
    rules.put(MinorType.DECIMAL28DENSE, rule);

    /** DECIMAL28Sparse cast able from **/

    rule = new HashSet<>();
    rule.add(MinorType.DECIMAL28SPARSE);
    rule.add(MinorType.VARDECIMAL);
    rules.put(MinorType.DECIMAL28SPARSE, rule);

    /* VARDECIMAL cast able from **/
    rule = new HashSet<>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.UINT1);
    rule.add(MinorType.UINT2);
    rule.add(MinorType.UINT4);
    rule.add(MinorType.UINT8);
    rule.add(MinorType.VARDECIMAL);
    rules.put(MinorType.VARDECIMAL, rule);

    /** DECIMAL38Dense cast able from **/
    rule = new HashSet<>();
    rule.add(MinorType.DECIMAL38DENSE);
    rule.add(MinorType.VARDECIMAL);
    rules.put(MinorType.DECIMAL38DENSE, rule);


    /** DECIMAL38Sparse cast able from **/
    rule = new HashSet<>();
    rule.add(MinorType.DECIMAL38SPARSE);
    rule.add(MinorType.VARDECIMAL);
    rules.put(MinorType.DECIMAL38SPARSE, rule);

    /** MONEY cast able from **/
    rule = new HashSet<>();
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
    rule.add(MinorType.VARDECIMAL);
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
    rule = new HashSet<>();
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
    rule = new HashSet<>();
    rule.add(MinorType.TIME);
    rule.add(MinorType.DATE);
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
    rule = new HashSet<>();
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
    rule.add(MinorType.TIMESTAMPTZ);
    rules.put(MinorType.TIMESTAMP, rule);

    /** TIMESTAMPTZ cast able from **/
    rule = new HashSet<>();
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
    rule = new HashSet<>();
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
    rule = new HashSet<>();
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
    rule = new HashSet<>();
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
    rule = new HashSet<>();
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
    rule.add(MinorType.VARDECIMAL);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rules.put(MinorType.FLOAT4, rule);

    /** FLOAT8 cast able from **/
    rule = new HashSet<>();
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
    rule.add(MinorType.VARDECIMAL);
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
    rule = new HashSet<>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.BIT);
    rule.add(MinorType.FIXEDCHAR);
    rule.add(MinorType.FIXED16CHAR);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rules.put(MinorType.BIT, rule);

    /** FIXEDCHAR cast able from **/
    rule = new HashSet<>();
    rule.add(MinorType.TINYINT);
    rule.add(MinorType.SMALLINT);
    rule.add(MinorType.INT);
    rule.add(MinorType.BIGINT);
    rule.add(MinorType.MONEY);
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
    rule.add(MinorType.VARDECIMAL);
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
    rule = new HashSet<>();
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
    rule.add(MinorType.VARDECIMAL);
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
    rules.put(MinorType.FIXED16CHAR, rule);

    /** FIXEDBINARY cast able from **/
    rule = new HashSet<>();
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
    rule.add(MinorType.VARDECIMAL);
    rule.add(MinorType.MONEY);
    rule.add(MinorType.FLOAT4);
    rule.add(MinorType.FLOAT8);
    rule.add(MinorType.BIT);
    rule.add(MinorType.VARCHAR);
    rule.add(MinorType.VAR16CHAR);
    rule.add(MinorType.VARBINARY);
    rule.add(MinorType.FIXEDBINARY);
    rules.put(MinorType.FIXEDBINARY, rule);

    /** VARCHAR cast able from **/
    rule = new HashSet<>();
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
    rule.add(MinorType.VARDECIMAL);
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
    rule = new HashSet<>();
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
    rule.add(MinorType.VARDECIMAL);
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
    rule = new HashSet<>();
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
    rule.add(MinorType.VARDECIMAL);
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

    rules.put(MinorType.MAP, Sets.newHashSet(MinorType.MAP));
    rules.put(MinorType.LIST, Sets.newHashSet(MinorType.LIST));
    rules.put(MinorType.UNION, Sets.newHashSet(MinorType.UNION));
    rules.put(MinorType.DICT, Sets.newHashSet(MinorType.DICT));
  }

  public static boolean isCastableWithNullHandling(MajorType argumentType, MajorType paramType, NullHandling nullHandling) {
    if ((argumentType.getMode() == DataMode.REPEATED || paramType.getMode() == DataMode.REPEATED) && argumentType.getMode() != paramType.getMode()) {
      return false;
    }
    // allow using functions with nullable parameters for required arguments only for the case of data mode mismatch
    if (nullHandling == NullHandling.INTERNAL && argumentType.getMode() != paramType.getMode()
        && (paramType.getMode() != DataMode.OPTIONAL || argumentType.getMode() != DataMode.REQUIRED)) {
      return false;
    }
    return isCastable(argumentType.getMinorType(), paramType.getMinorType());
  }

  public static boolean isCastable(MinorType from, MinorType to) {
    return from.equals(MinorType.NULL) ||      //null could be casted to any other type.
        (rules.get(to) != null && rules.get(to).contains(from));
  }

  public static DataMode getLeastRestrictiveDataMode(DataMode... dataModes) {
    boolean hasOptional = false;
    for(DataMode dataMode : dataModes) {
      switch (dataMode) {
        case REPEATED:
          return dataMode;
        case OPTIONAL:
          hasOptional = true;
        default:
      }
    }

    if(hasOptional) {
      return DataMode.OPTIONAL;
    } else {
      return DataMode.REQUIRED;
    }
  }

  public static MinorType getLeastRestrictiveType(MinorType... types) {
    MinorType result = types[0];
    if (result == MinorType.UNION) {
      return result;
    }

    for (int i = 1; i < types.length; i++) {
      MinorType next = types[i];
      if (next == MinorType.UNION) {
        return next;
      }
      if (next == result) {
        // both args are of the same type; continue
        continue;
      }

      float resultCastCost = ResolverTypePrecedence.computeCost(next, result);
      float nextCastCost = ResolverTypePrecedence.computeCost(result, next);

      if (isCastable(next, result) && resultCastCost <= nextCastCost && resultCastCost < Float.POSITIVE_INFINITY) {
        continue;
      } else if (isCastable(result, next) && nextCastCost <= resultCastCost && nextCastCost < Float.POSITIVE_INFINITY) {
        result = next;
      } else {
        return null;
      }
    }

    return result;
  }

  /**
   * Finds the type in a given set that has the cheapest cast from a given
   * starting type.
   * @param fromType type to cast from.
   * @param toTypes candidate types to cast to.
   * @return the type in toTypes that has the cheapest cast or empty if no
   *         finite cost cast can be found.
   */
  public static Optional<MinorType> getCheapestCast(MinorType fromType, MinorType... toTypes) {
    MinorType cheapest = null;
    float cheapestCost = Float.POSITIVE_INFINITY;

    for (MinorType toType: toTypes) {
      float toTypeCost = ResolverTypePrecedence.computeCost(fromType, toType);
      if (toTypeCost < cheapestCost) {
        cheapest = toType;
        cheapestCost = toTypeCost;
      }
    }

    return Optional.ofNullable(cheapest);
  }

  // cost of changing mode from required to optional
  private static final float DATAMODE_CHANGE_COST = 1f;
  // cost of casting to a field reader, compare to edge weights in ResolverTypePrecedence
  private static final float FIELD_READER_COST = 100f;
  // cost of matching a vararg function, compare to edge weights in ResolverTypePrecedence
  private static final float VARARG_COST = 100f;

  /**
   * Decide whether it's legal to do implicit cast.
   * @returns
   *      0: param types equal arg types, no casting needed.
   * (0,+∞): the cost of implicit casting.
   *     +∞: implicit casting is not allowed.
   */
  public static float getCost(List<MajorType> argumentTypes, DrillFuncHolder holder) {
    // Running sum of casting cost.
    float totalCost = 0;

    if (argumentTypes.size() != holder.getParamCount() && !holder.isVarArg()) {
      return Float.POSITIVE_INFINITY;
    }

    /*
     * If we are determining function holder for decimal data type, we need to
     * make sure the output type of the function can fit the precision that we
     * need based on the input types.
     */
    if (holder.checkPrecisionRange()) {
      List<LogicalExpression> logicalExpressions = Lists.newArrayList();
      for(MajorType majorType : argumentTypes) {
        logicalExpressions.add(
            new MajorTypeInLogicalExpression(majorType));
      }

      if (DRILL_REL_DATATYPE_SYSTEM.getMaxNumericPrecision() <
          holder.getReturnType(logicalExpressions).getPrecision()) {
        return Float.POSITIVE_INFINITY;
      }
    }

    final int numOfArgs = argumentTypes.size();
    for (int i = 0; i < numOfArgs; i++) {
      final MajorType argType = argumentTypes.get(i);
      final MajorType paramType = holder.getParamMajorType(i);

      //@Param FieldReader will match any type
      if (holder.isFieldReader(i)) {
//        if (Types.isComplex(call.args.get(i).getMajorType()) ||Types.isRepeated(call.args.get(i).getMajorType()) )
        // add a weighted cost when we encounter a field reader considering
        // that it is the most expensive factor contributing to the cost.
        totalCost += FIELD_READER_COST;
        continue;
      }

      if (!TypeCastRules.isCastableWithNullHandling(argType, paramType, holder.getNullHandling())) {
        // one uncastable argument is enough to kill the party
        return Float.POSITIVE_INFINITY;
      }

      float castCost = ResolverTypePrecedence.computeCost(
        argType.getMinorType(),
        paramType.getMinorType()
      );

      if (castCost == Float.POSITIVE_INFINITY) {
        // A single uncastable argument is enough to kill the party
        return Float.POSITIVE_INFINITY;
      }

      totalCost += castCost;
      totalCost += holder.getNullHandling() == NullHandling.INTERNAL && paramType.getMode() != argType.getMode()
        ? DATAMODE_CHANGE_COST
        : 0;

    }

    if (holder.isVarArg()) {
      int varArgIndex = holder.getParamCount() - 1;
      for (int i = varArgIndex; i < numOfArgs; i++) {
        if (holder.isFieldReader(varArgIndex)) {
          break;
        } else if (holder.getParamMajorType(varArgIndex).getMode() == DataMode.REQUIRED
            && holder.getParamMajorType(varArgIndex).getMode() != argumentTypes.get(i).getMode()) {
          // prohibit using vararg functions for types with different nullability
          // if function accepts required arguments, but provided optional
          return Float.POSITIVE_INFINITY;
        }
      }
      // increase cost for var arg functions to prioritize regular ones
      totalCost += VARARG_COST;
      totalCost += ResolverTypePrecedence.computeCost(
        MinorType.NULL,
        holder.getParamMajorType(varArgIndex).getMinorType()
      );
      totalCost += holder.getParamMajorType(varArgIndex).getMode() == DataMode.REQUIRED ? 0 : 1;
    }

    return totalCost;
  }

  /*
   * Simple helper function to determine if input type is numeric
   */
  public static boolean isNumericType(MinorType inputType) {
    switch (inputType) {
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
      case UINT1:
      case UINT2:
      case UINT4:
      case UINT8:
      case DECIMAL9:
      case DECIMAL18:
      case DECIMAL28SPARSE:
      case DECIMAL38SPARSE:
      case VARDECIMAL:
      case FLOAT4:
      case FLOAT8:
        return true;
      default:
        return false;
    }
  }
}
