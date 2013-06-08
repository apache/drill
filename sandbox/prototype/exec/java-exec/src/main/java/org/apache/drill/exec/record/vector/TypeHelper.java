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
package org.apache.drill.exec.record.vector;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.SchemaDefProtos.DataMode;
import org.apache.drill.exec.proto.SchemaDefProtos.MajorType;
import org.apache.drill.exec.proto.SchemaDefProtos.MinorType;
import org.apache.drill.exec.record.MaterializedField;

public class TypeHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeHelper.class);

  private static final int WIDTH_ESTIMATE_1 = 10;
  private static final int WIDTH_ESTIMATE_2 = 50000;
  private static final int WIDTH_ESTIMATE_4 = 1024*1024;

  public static int getSize(MajorType major){
    switch(major.getMinorType()){
    case TINYINT: return 1;
    case SMALLINT: return 2;
    case INT: return 4;
    case BIGINT: return 8;
    case DECIMAL4: return 4;
    case DECIMAL8: return 8;
    case DECIMAL12: return 12;
    case DECIMAL16: return 16;
    case MONEY: return 8;
    case DATE: return 4;
    case TIME: return 8;
    case TIMETZ: return 12;
    case TIMESTAMP: return 8;
    case DATETIME: return 8;
    case INTERVAL: return 12;
    case FLOAT4: return 4;
    case FLOAT8: return 8;
    case BOOLEAN: return 1/8;
    case FIXEDCHAR: return major.getWidth();
    case VARCHAR1: return 1 + WIDTH_ESTIMATE_1;
    case VARCHAR2: return 2 + WIDTH_ESTIMATE_2;
    case VARCHAR4: return 4 + WIDTH_ESTIMATE_4;
    case FIXEDBINARY: return major.getWidth();
    case VARBINARY1: return 1 + WIDTH_ESTIMATE_1;
    case VARBINARY2: return 2 + WIDTH_ESTIMATE_2;
    case VARBINARY4: return 4 + WIDTH_ESTIMATE_4;
    case UINT1: return 1;
    case UINT2: return 2;
    case UINT4: return 4;
    case UINT8: return 8;
    case PROTO2: return 2 + WIDTH_ESTIMATE_2;
    case PROTO4: return 4 + WIDTH_ESTIMATE_4;
    case MSGPACK2: return 2 + WIDTH_ESTIMATE_2;
    case MSGPACK4: return 4 + WIDTH_ESTIMATE_4;
    }
    return 4;
  }

  public static Class<?> getValueVectorClass(MinorType type, DataMode mode){
    switch(mode){
    case OPTIONAL:
      switch(type){
        case REPEATMAP: return RepeatMap.class;
        case TINYINT: return Fixed1.class;
        case SMALLINT: return Fixed2.class;
        case INT: return Fixed4.class;
        case BIGINT: return Fixed8.class;
        case DECIMAL4: return Fixed4.class;
        case DECIMAL8: return Fixed8.class;
        case DECIMAL12: return Fixed12.class;
        case DECIMAL16: return Fixed16.class;
        case MONEY: return Fixed8.class;
        case DATE: return Fixed4.class;
        case TIME: return Fixed8.class;
        case TIMETZ: return Fixed12.class;
        case TIMESTAMP: return Fixed8.class;
        case DATETIME: return Fixed8.class;
        case INTERVAL: return Fixed12.class;
        case FLOAT4: return Fixed4.class;
        case FLOAT8: return Fixed8.class;
        case BOOLEAN: return Bit.class;
        case FIXEDCHAR: return FixedLen.class;
        case VARCHAR1: return VarLen1.class;
        case VARCHAR2: return VarLen2.class;
        case VARCHAR4: return VarLen4.class;
        case FIXEDBINARY: return FixedLen.class;
        case VARBINARY1: return VarLen1.class;
        case VARBINARY2: return VarLen2.class;
        case VARBINARY4: return VarLen4.class;
        case UINT1: return Fixed1.class;
        case UINT2: return Fixed2.class;
        case UINT4: return Fixed4.class;
        case UINT8: return Fixed8.class;
        case PROTO2: return VarLen2.class;
        case PROTO4: return VarLen4.class;
        case MSGPACK2: return VarLen2.class;
        case MSGPACK4: return VarLen4.class;
      }
      break;
    case REQUIRED:
      switch(type){
//        case TINYINT: return NullableFixed1.class;
//        case SMALLINT: return NullableFixed2.class;
//        case INT: return NullableFixed4.class;
//        case BIGINT: return NullableFixed8.class;
//        case DECIMAL4: return NullableFixed4.class;
//        case DECIMAL8: return NullableFixed8.class;
//        case DECIMAL12: return NullableFixed12.class;
//        case DECIMAL16: return NullableFixed16.class;
//        case MONEY: return NullableFixed8.class;
//        case DATE: return NullableFixed4.class;
//        case TIME: return NullableFixed8.class;
//        case TIMETZ: return NullableFixed12.class;
//        case TIMESTAMP: return NullableFixed8.class;
//        case DATETIME: return NullableFixed8.class;
//        case INTERVAL: return NullableFixed12.class;
//        case FLOAT4: return NullableFixed4.class;
//        case FLOAT8: return NullableFixed8.class;
//        case BOOLEAN: return NullableBit.class;
//        case FIXEDCHAR: return NullableFixedLen.class;
//        case VARCHAR1: return NullableVarLen1.class;
//        case VARCHAR2: return NullableVarLen2.class;
//        case VARCHAR4: return NullableVarLen4.class;
//        case FIXEDBINARY: return NullableFixedLen.class;
//        case VARBINARY1: return NullableVarLen1.class;
//        case VARBINARY2: return NullableVarLen2.class;
//        case VARBINARY4: return NullableVarLen4.class;
//        case UINT1: return NullableFixed1.class;
//        case UINT2: return NullableFixed2.class;
//        case UINT4: return NullableFixed4.class;
//        case UINT8: return NullableFixed8.class;
//        case PROTO2: return NullableVarLen2.class;
//        case PROTO4: return NullableVarLen4.class;
//        case MSGPACK2: return NullableVarLen2.class;
//        case MSGPACK4: return NullableVarLen4.class;      
      }
      break;
    case REPEATED:
      switch(type){
//        case TINYINT: return RepeatedFixed1.class;
//        case SMALLINT: return RepeatedFixed2.class;
//        case INT: return RepeatedFixed4.class;
//        case BIGINT: return RepeatedFixed8.class;
//        case DECIMAL4: return RepeatedFixed4.class;
//        case DECIMAL8: return RepeatedFixed8.class;
//        case DECIMAL12: return RepeatedFixed12.class;
//        case DECIMAL16: return RepeatedFixed16.class;
//        case MONEY: return RepeatedFixed8.class;
//        case DATE: return RepeatedFixed4.class;
//        case TIME: return RepeatedFixed8.class;
//        case TIMETZ: return RepeatedFixed12.class;
//        case TIMESTAMP: return RepeatedFixed8.class;
//        case DATETIME: return RepeatedFixed8.class;
//        case INTERVAL: return RepeatedFixed12.class;
//        case FLOAT4: return RepeatedFixed4.class;
//        case FLOAT8: return RepeatedFixed8.class;
//        case BOOLEAN: return RepeatedBit.class;
//        case FIXEDCHAR: return RepeatedFixedLen.class;
//        case VARCHAR1: return RepeatedVarLen1.class;
//        case VARCHAR2: return RepeatedVarLen2.class;
//        case VARCHAR4: return RepeatedVarLen4.class;
//        case FIXEDBINARY: return RepeatedFixedLen.class;
//        case VARBINARY1: return RepeatedVarLen1.class;
//        case VARBINARY2: return RepeatedVarLen2.class;
//        case VARBINARY4: return RepeatedVarLen4.class;
//        case UINT1: return RepeatedFixed1.class;
//        case UINT2: return RepeatedFixed2.class;
//        case UINT4: return RepeatedFixed4.class;
//        case UINT8: return RepeatedFixed8.class;
//        case PROTO2: return RepeatedVarLen2.class;
//        case PROTO4: return RepeatedVarLen4.class;
//        case MSGPACK2: return RepeatedVarLen2.class;
//        case MSGPACK4: return RepeatedVarLen4.class;      
      }
      break;
    default:
      break;

    }
    throw new UnsupportedOperationException();
  }


  public static ValueVector<?> getNewVector(MaterializedField field, BufferAllocator allocator){
    MajorType type = field.getType();
    switch(type.getMode()){
    case REQUIRED:
      switch(type.getMinorType()){
      case TINYINT: return new Fixed1(field, allocator);
      case SMALLINT: return new Fixed2(field, allocator);
      case INT: return new Fixed4(field, allocator);
      case BIGINT: return new Fixed8(field, allocator);
      case DECIMAL4: return new Fixed4(field, allocator);
      case DECIMAL8: return new Fixed8(field, allocator);
      case DECIMAL12: return new Fixed12(field, allocator);
      case DECIMAL16: return new Fixed16(field, allocator);
      case MONEY: return new Fixed8(field, allocator);
      case DATE: return new Fixed4(field, allocator);
      case TIME: return new Fixed8(field, allocator);
      case TIMETZ: return new Fixed12(field, allocator);
      case TIMESTAMP: return new Fixed8(field, allocator);
      case DATETIME: return new Fixed8(field, allocator);
      case INTERVAL: return new Fixed12(field, allocator);
      case FLOAT4: return new Fixed4(field, allocator);
      case FLOAT8: return new Fixed8(field, allocator);
      case BOOLEAN: return new Bit(field, allocator);
      case FIXEDCHAR: return new FixedLen(field, allocator);
      case VARCHAR1: return new VarLen1(field, allocator);
      case VARCHAR2: return new VarLen2(field, allocator);
      case VARCHAR4: return new VarLen4(field, allocator);
      case FIXEDBINARY: return new FixedLen(field, allocator);
      case VARBINARY1: return new VarLen1(field, allocator);
      case VARBINARY2: return new VarLen2(field, allocator);
      case VARBINARY4: return new VarLen4(field, allocator);
      case UINT1: return new Fixed1(field, allocator);
      case UINT2: return new Fixed2(field, allocator);
      case UINT4: return new Fixed4(field, allocator);
      case UINT8: return new Fixed8(field, allocator);
      case PROTO2: return new VarLen2(field, allocator);
      case PROTO4: return new VarLen4(field, allocator);
      case MSGPACK2: return new VarLen2(field, allocator);
      case MSGPACK4: return new VarLen4(field, allocator);
      }
      break;
    case REPEATED:
        switch(type.getMinorType()) {
            case MAP: return new RepeatMap(field, allocator);
        }
      break;
    case OPTIONAL:
        switch(type.getMinorType()) {
            case BOOLEAN: return new NullableBit(field, allocator);
            case INT: return new NullableFixed4(field, allocator);
            case FLOAT4: return new NullableFixed4(field, allocator);
            case VARCHAR4: return new NullableVarLen4(field, allocator);
        }
      break;
    default:
      break;

    }
    throw new UnsupportedOperationException(type.getMinorType() + " type is not supported. Mode: " + type.getMode());
  }

}
