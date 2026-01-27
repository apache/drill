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
package org.apache.drill.exec.store.paimon.read;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.record.ColumnConverter;
import org.apache.drill.exec.record.ColumnConverterFactory;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.vector.accessor.ValueWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.LocalZoneTimestamp;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;

import java.math.BigDecimal;
import java.time.Instant;

public class PaimonColumnConverterFactory extends ColumnConverterFactory {

  public PaimonColumnConverterFactory(TupleMetadata providedSchema) {
    super(providedSchema);
  }

  @Override
  public ColumnConverter.ScalarColumnConverter buildScalar(ColumnMetadata readerSchema, ValueWriter writer) {
    switch (readerSchema.type()) {
      case TIMESTAMP:
      case TIMESTAMPTZ:
        return new ColumnConverter.ScalarColumnConverter(value -> writer.setTimestamp(asInstant(value)));
      case VARDECIMAL:
        return new ColumnConverter.ScalarColumnConverter(value -> writer.setDecimal(asBigDecimal(value)));
      case VARCHAR:
        return new ColumnConverter.ScalarColumnConverter(value -> writer.setString(asString(value)));
      case VARBINARY:
        return new ColumnConverter.ScalarColumnConverter(value -> {
          byte[] bytes = (byte[]) value;
          writer.setBytes(bytes, bytes.length);
        });
      default:
        return super.buildScalar(readerSchema, writer);
    }
  }

  public static ColumnMetadata getColumnMetadata(DataField field) {
    DataType type = field.type();
    String name = field.name();
    TypeProtos.DataMode dataMode = type.isNullable()
      ? TypeProtos.DataMode.OPTIONAL
      : TypeProtos.DataMode.REQUIRED;
    return getColumnMetadata(name, type, dataMode);
  }

  public static TupleSchema convertSchema(RowType rowType) {
    TupleSchema schema = new TupleSchema();
    for (DataField field : rowType.getFields()) {
      ColumnMetadata columnMetadata = getColumnMetadata(field);
      schema.add(columnMetadata);
    }
    return schema;
  }

  private static ColumnMetadata getColumnMetadata(String name, DataType type, TypeProtos.DataMode dataMode) {
    DataTypeRoot typeRoot = type.getTypeRoot();
    switch (typeRoot) {
      case ARRAY:
      case MAP:
      case MULTISET:
      case ROW:
      case VARIANT:
        throw new UnsupportedOperationException(String.format("Unsupported type: %s for column: %s", typeRoot, name));
      default:
        return getPrimitiveMetadata(name, type, dataMode);
    }
  }

  private static ColumnMetadata getPrimitiveMetadata(String name, DataType type, TypeProtos.DataMode dataMode) {
    TypeProtos.MinorType minorType = getType(type.getTypeRoot());
    if (minorType == null) {
      throw new UnsupportedOperationException(String.format("Unsupported type: %s for column: %s", type.getTypeRoot(), name));
    }
    TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder()
      .setMinorType(minorType)
      .setMode(dataMode);
    if (type.getTypeRoot() == DataTypeRoot.DECIMAL) {
      DecimalType decimalType = (DecimalType) type;
      builder.setScale(decimalType.getScale())
        .setPrecision(decimalType.getPrecision());
    }
    MaterializedField materializedField = MaterializedField.create(name, builder.build());
    return MetadataUtils.fromField(materializedField);
  }

  private static TypeProtos.MinorType getType(DataTypeRoot typeRoot) {
    switch (typeRoot) {
      case BOOLEAN:
        return TypeProtos.MinorType.BIT;
      case TINYINT:
        return TypeProtos.MinorType.TINYINT;
      case SMALLINT:
        return TypeProtos.MinorType.SMALLINT;
      case INTEGER:
        return TypeProtos.MinorType.INT;
      case BIGINT:
        return TypeProtos.MinorType.BIGINT;
      case FLOAT:
        return TypeProtos.MinorType.FLOAT4;
      case DOUBLE:
        return TypeProtos.MinorType.FLOAT8;
      case DATE:
        return TypeProtos.MinorType.DATE;
      case TIME_WITHOUT_TIME_ZONE:
        return TypeProtos.MinorType.TIME;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return TypeProtos.MinorType.TIMESTAMP;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TypeProtos.MinorType.TIMESTAMPTZ;
      case CHAR:
      case VARCHAR:
        return TypeProtos.MinorType.VARCHAR;
      case BINARY:
      case VARBINARY:
        return TypeProtos.MinorType.VARBINARY;
      case DECIMAL:
        return TypeProtos.MinorType.VARDECIMAL;
      default:
        return null;
    }
  }

  private static String asString(Object value) {
    if (value instanceof BinaryString) {
      return value.toString();
    }
    return (String) value;
  }

  private static BigDecimal asBigDecimal(Object value) {
    if (value instanceof Decimal) {
      return ((Decimal) value).toBigDecimal();
    }
    return (BigDecimal) value;
  }

  private static Instant asInstant(Object value) {
    if (value instanceof Timestamp) {
      return ((Timestamp) value).toInstant();
    }
    if (value instanceof LocalZoneTimestamp) {
      return ((LocalZoneTimestamp) value).toInstant();
    }
    if (value instanceof java.sql.Timestamp) {
      return ((java.sql.Timestamp) value).toInstant();
    }
    if (value instanceof Long) {
      return Instant.ofEpochMilli((Long) value);
    }
    return (Instant) value;
  }
}
