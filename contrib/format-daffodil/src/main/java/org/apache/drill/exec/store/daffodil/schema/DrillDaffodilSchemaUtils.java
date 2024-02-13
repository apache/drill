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

package org.apache.drill.exec.store.daffodil.schema;

import org.apache.daffodil.japi.InvalidParserException;
import org.apache.daffodil.japi.DataProcessor;
import org.apache.daffodil.runtime1.api.JPrimType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class DrillDaffodilSchemaUtils {
  private static final MinorType DEFAULT_TYPE = MinorType.VARCHAR;
  private static final Logger logger = LoggerFactory.getLogger(DrillDaffodilSchemaUtils.class);

  /**
   * This map maps the data types defined by the DFDL definition to Drill data types.
   */
  public static final ImmutableMap<JPrimType, MinorType> DFDL_TYPE_MAPPINGS =
      ImmutableMap.<JPrimType, MinorType>builder()
          .put(JPrimType.Long, MinorType.BIGINT)
          .put(JPrimType.Int, MinorType.INT)
          .put(JPrimType.Short, MinorType.SMALLINT)
          .put(JPrimType.Byte, MinorType.TINYINT)
          // daffodil unsigned longs are modeled as DECIMAL(38, 0) which is the default for VARDECIMAL
          .put(JPrimType.UnsignedLong, MinorType.VARDECIMAL)
          .put(JPrimType.UnsignedInt, MinorType.BIGINT)
          .put(JPrimType.UnsignedShort, MinorType.UINT2)
          .put(JPrimType.UnsignedByte, MinorType.UINT1)
          // daffodil integer, nonNegativeInteger, are modeled as DECIMAL(38, 0) which is the default for VARDECIMAL
          .put(JPrimType.Integer, MinorType.VARDECIMAL)
          .put(JPrimType.NonNegativeInteger, MinorType.VARDECIMAL)
          // decimal has to be modeled as string since we really have no idea what to set the
          // scale to.
          .put(JPrimType.Decimal, MinorType.VARCHAR)
          .put(JPrimType.Boolean, MinorType.BIT)
          .put(JPrimType.Date, MinorType.DATE) // requires conversion
          .put(JPrimType.DateTime, MinorType.TIMESTAMP) // requires conversion
          .put(JPrimType.Double, MinorType.FLOAT8)
          //
          // daffodil float type is mapped to double aka Float8 in drill because there
          // seems to be bugs in FLOAT4. Float.MaxValue in a Float4 column displays as
          // 3.4028234663852886E38 not 3.4028235E38.
          //
          // We don't really care about single float precision, so we just use double precision.
          //
          .put(JPrimType.Float, MinorType.FLOAT8)
          .put(JPrimType.HexBinary, MinorType.VARBINARY)
          .put(JPrimType.String, MinorType.VARCHAR)
          .put(JPrimType.Time, MinorType.TIME) // requires conversion
          .build();


  @VisibleForTesting
  public static TupleMetadata processSchema(URI dfdlSchemaURI, String rootName, String namespace)
      throws IOException, DaffodilDataProcessorFactory.CompileFailure,
      URISyntaxException, InvalidParserException {
    DaffodilDataProcessorFactory dpf = new DaffodilDataProcessorFactory();
    boolean validationMode = true; // use Daffodil's limited validation always
    DataProcessor dp = dpf.getDataProcessor(dfdlSchemaURI, validationMode, rootName, namespace);
    return daffodilDataProcessorToDrillSchema(dp);
  }

  public static TupleMetadata daffodilDataProcessorToDrillSchema(DataProcessor dp) {
    DrillDaffodilSchemaVisitor schemaVisitor = new DrillDaffodilSchemaVisitor();
    dp.walkMetadata(schemaVisitor);
    TupleMetadata drillSchema = schemaVisitor.getDrillSchema();
    return drillSchema;
  }

  /**
   * Returns a {@link MinorType} of the corresponding DFDL Data Type.  Defaults to VARCHAR if unknown
   * @param dfdlType The type as provided by Daffodil.
   * @return A {@link MinorType} of the Drill data type.
   */
  public static MinorType getDrillDataType(JPrimType dfdlType) {
    try {
      MinorType type = DrillDaffodilSchemaUtils.DFDL_TYPE_MAPPINGS.get(dfdlType);
      if (type == null) {
        return DEFAULT_TYPE;
      } else {
        return type;
      }
    } catch (NullPointerException e) {
        logger.warn("Unknown data type found in XSD reader: {}.  Returning VARCHAR.", dfdlType);
      return DEFAULT_TYPE;
    }
  }
}
