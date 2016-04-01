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
package org.apache.drill.exec.store.hive;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableDecimal18Vector;
import org.apache.drill.exec.vector.NullableDecimal28SparseVector;
import org.apache.drill.exec.vector.NullableDecimal38SparseVector;
import org.apache.drill.exec.vector.NullableDecimal9Vector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.work.ExecErrorConstants;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Properties;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;

public class HiveUtilities {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveUtilities.class);

  /** Partition value is received in string format. Convert it into appropriate object based on the type. */
  public static Object convertPartitionType(TypeInfo typeInfo, String value, final String defaultPartitionValue) {
    if (typeInfo.getCategory() != Category.PRIMITIVE) {
      // In Hive only primitive types are allowed as partition column types.
      throw new DrillRuntimeException("Non-Primitive types are not allowed as partition column type in Hive, " +
          "but received one: " + typeInfo.getCategory());
    }

    if (defaultPartitionValue.equals(value)) {
      return null;
    }

    final PrimitiveCategory pCat = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();

    try {
      switch (pCat) {
        case BINARY:
          return value.getBytes();
        case BOOLEAN:
          return Boolean.parseBoolean(value);
        case DECIMAL: {
          DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) typeInfo;
          return HiveDecimalUtils.enforcePrecisionScale(HiveDecimal.create(value),
              decimalTypeInfo.precision(), decimalTypeInfo.scale());
        }
        case DOUBLE:
          return Double.parseDouble(value);
        case FLOAT:
          return Float.parseFloat(value);
        case BYTE:
        case SHORT:
        case INT:
          return Integer.parseInt(value);
        case LONG:
          return Long.parseLong(value);
        case STRING:
        case VARCHAR:
          return value.getBytes();
        case CHAR:
          return value.trim().getBytes();
        case TIMESTAMP:
          return Timestamp.valueOf(value);
        case DATE:
          return Date.valueOf(value);
      }
    } catch(final Exception e) {
      // In Hive, partition values that can't be converted from string are considered to be NULL.
      logger.trace("Failed to interpret '{}' value from partition value string '{}'", pCat, value);
      return null;
    }

    throwUnsupportedHiveDataTypeError(pCat.toString());
    return null;
  }

  public static void populateVector(final ValueVector vector, final DrillBuf managedBuffer, final Object val,
      final int start, final int end) {
    TypeProtos.MinorType type = vector.getField().getType().getMinorType();

    switch(type) {
      case VARBINARY: {
        NullableVarBinaryVector v = (NullableVarBinaryVector) vector;
        byte[] value = (byte[]) val;
        for (int i = start; i < end; i++) {
          v.getMutator().setSafe(i, value, 0, value.length);
        }
        break;
      }
      case BIT: {
        NullableBitVector v = (NullableBitVector) vector;
        Boolean value = (Boolean) val;
        for (int i = start; i < end; i++) {
          v.getMutator().set(i, value ? 1 : 0);
        }
        break;
      }
      case FLOAT8: {
        NullableFloat8Vector v = (NullableFloat8Vector) vector;
        double value = (double) val;
        for (int i = start; i < end; i++) {
          v.getMutator().setSafe(i, value);
        }
        break;
      }
      case FLOAT4: {
        NullableFloat4Vector v = (NullableFloat4Vector) vector;
        float value = (float) val;
        for (int i = start; i < end; i++) {
          v.getMutator().setSafe(i, value);
        }
        break;
      }
      case TINYINT:
      case SMALLINT:
      case INT: {
        NullableIntVector v = (NullableIntVector) vector;
        int value = (int) val;
        for (int i = start; i < end; i++) {
          v.getMutator().setSafe(i, value);
        }
        break;
      }
      case BIGINT: {
        NullableBigIntVector v = (NullableBigIntVector) vector;
        long value = (long) val;
        for (int i = start; i < end; i++) {
          v.getMutator().setSafe(i, value);
        }
        break;
      }
      case VARCHAR: {
        NullableVarCharVector v = (NullableVarCharVector) vector;
        byte[] value = (byte[]) val;
        for (int i = start; i < end; i++) {
          v.getMutator().setSafe(i, value, 0, value.length);
        }
        break;
      }
      case TIMESTAMP: {
        NullableTimeStampVector v = (NullableTimeStampVector) vector;
        DateTime ts = new DateTime(((Timestamp) val).getTime()).withZoneRetainFields(DateTimeZone.UTC);
        long value = ts.getMillis();
        for (int i = start; i < end; i++) {
          v.getMutator().setSafe(i, value);
        }
        break;
      }
      case DATE: {
        NullableDateVector v = (NullableDateVector) vector;
        DateTime date = new DateTime(((Date)val).getTime()).withZoneRetainFields(DateTimeZone.UTC);
        long value = date.getMillis();
        for (int i = start; i < end; i++) {
          v.getMutator().setSafe(i, value);
        }
        break;
      }

      case DECIMAL9: {
        final BigDecimal value = ((HiveDecimal)val).bigDecimalValue();
        final NullableDecimal9Vector v = ((NullableDecimal9Vector) vector);
        final Decimal9Holder holder = new Decimal9Holder();
        holder.scale = v.getField().getScale();
        holder.precision = v.getField().getPrecision();
        holder.value = DecimalUtility.getDecimal9FromBigDecimal(value, holder.scale, holder.precision);
        for (int i = start; i < end; i++) {
          v.getMutator().setSafe(i, holder);
        }
        break;
      }

      case DECIMAL18: {
        final BigDecimal value = ((HiveDecimal)val).bigDecimalValue();
        final NullableDecimal18Vector v = ((NullableDecimal18Vector) vector);
        final Decimal18Holder holder = new Decimal18Holder();
        holder.scale = v.getField().getScale();
        holder.precision = v.getField().getPrecision();
        holder.value = DecimalUtility.getDecimal18FromBigDecimal(value, holder.scale, holder.precision);
        for (int i = start; i < end; i++) {
          v.getMutator().setSafe(i, holder);
        }
        break;
      }

      case DECIMAL28SPARSE: {
      final int needSpace = Decimal28SparseHolder.nDecimalDigits * DecimalUtility.INTEGER_SIZE;
        Preconditions.checkArgument(managedBuffer.capacity() > needSpace,
            String.format("Not sufficient space in given managed buffer. Need %d bytes, buffer has %d bytes",
                needSpace, managedBuffer.capacity()));

        final BigDecimal value = ((HiveDecimal)val).bigDecimalValue();
        final NullableDecimal28SparseVector v = ((NullableDecimal28SparseVector) vector);
        final Decimal28SparseHolder holder = new Decimal28SparseHolder();
        holder.scale = v.getField().getScale();
        holder.precision = v.getField().getPrecision();
        holder.buffer = managedBuffer;
        holder.start = 0;
        DecimalUtility.getSparseFromBigDecimal(value, holder.buffer, 0, holder.scale, holder.precision,
            Decimal28SparseHolder.nDecimalDigits);
        for (int i = start; i < end; i++) {
          v.getMutator().setSafe(i, holder);
        }
        break;
      }

      case DECIMAL38SPARSE: {
      final int needSpace = Decimal38SparseHolder.nDecimalDigits * DecimalUtility.INTEGER_SIZE;
        Preconditions.checkArgument(managedBuffer.capacity() > needSpace,
            String.format("Not sufficient space in given managed buffer. Need %d bytes, buffer has %d bytes",
                needSpace, managedBuffer.capacity()));
        final BigDecimal value = ((HiveDecimal)val).bigDecimalValue();
        final NullableDecimal38SparseVector v = ((NullableDecimal38SparseVector) vector);
        final Decimal38SparseHolder holder = new Decimal38SparseHolder();
        holder.scale = v.getField().getScale();
        holder.precision = v.getField().getPrecision();
        holder.buffer = managedBuffer;
        holder.start = 0;
        DecimalUtility.getSparseFromBigDecimal(value, holder.buffer, 0, holder.scale, holder.precision,
            Decimal38SparseHolder.nDecimalDigits);
        for (int i = start; i < end; i++) {
          v.getMutator().setSafe(i, holder);
        }
        break;
      }
    }
  }

  public static MajorType getMajorTypeFromHiveTypeInfo(final TypeInfo typeInfo, final OptionManager options) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE: {
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        MinorType minorType = HiveUtilities.getMinorTypeFromHivePrimitiveTypeInfo(primitiveTypeInfo, options);
        MajorType.Builder typeBuilder = MajorType.newBuilder().setMinorType(minorType)
            .setMode(DataMode.OPTIONAL); // Hive columns (both regular and partition) could have null values

        if (primitiveTypeInfo.getPrimitiveCategory() == PrimitiveCategory.DECIMAL) {
          DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
          typeBuilder.setPrecision(decimalTypeInfo.precision())
              .setScale(decimalTypeInfo.scale()).build();
        }

        return typeBuilder.build();
      }

      case LIST:
      case MAP:
      case STRUCT:
      case UNION:
      default:
        throwUnsupportedHiveDataTypeError(typeInfo.getCategory().toString());
    }

    return null;
  }

  public static TypeProtos.MinorType getMinorTypeFromHivePrimitiveTypeInfo(PrimitiveTypeInfo primitiveTypeInfo,
                                                                           OptionManager options) {
    switch(primitiveTypeInfo.getPrimitiveCategory()) {
      case BINARY:
        return TypeProtos.MinorType.VARBINARY;
      case BOOLEAN:
        return TypeProtos.MinorType.BIT;
      case DECIMAL: {

        if (options.getOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY).bool_val == false) {
          throw UserException.unsupportedError()
              .message(ExecErrorConstants.DECIMAL_DISABLE_ERR_MSG)
              .build(logger);
        }
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
        return DecimalUtility.getDecimalDataType(decimalTypeInfo.precision());
      }
      case DOUBLE:
        return TypeProtos.MinorType.FLOAT8;
      case FLOAT:
        return TypeProtos.MinorType.FLOAT4;
      // TODO (DRILL-2470)
      // Byte and short (tinyint and smallint in SQL types) are currently read as integers
      // as these smaller integer types are not fully supported in Drill today.
      case SHORT:
      case BYTE:
      case INT:
        return TypeProtos.MinorType.INT;
      case LONG:
        return TypeProtos.MinorType.BIGINT;
      case STRING:
      case VARCHAR:
      case CHAR:
        return TypeProtos.MinorType.VARCHAR;
      case TIMESTAMP:
        return TypeProtos.MinorType.TIMESTAMP;
      case DATE:
        return TypeProtos.MinorType.DATE;
    }
    throwUnsupportedHiveDataTypeError(primitiveTypeInfo.getPrimitiveCategory().toString());
    return null;
  }

  /**
   * Utility method which gets table or partition {@link InputFormat} class. First it
   * tries to get the class name from given StorageDescriptor object. If it doesn't contain it tries to get it from
   * StorageHandler class set in table properties. If not found throws an exception.
   * @param job {@link JobConf} instance needed incase the table is StorageHandler based table.
   * @param sd {@link StorageDescriptor} instance of currently reading partition or table (for non-partitioned tables).
   * @param table Table object
   * @throws Exception
   */
  public static Class<? extends InputFormat<?, ?>> getInputFormatClass(final JobConf job, final StorageDescriptor sd,
      final Table table) throws Exception {
    final String inputFormatName = sd.getInputFormat();
    if (Strings.isNullOrEmpty(inputFormatName)) {
      final String storageHandlerClass = table.getParameters().get(META_TABLE_STORAGE);
      if (Strings.isNullOrEmpty(storageHandlerClass)) {
        throw new ExecutionSetupException("Unable to get Hive table InputFormat class. There is neither " +
            "InputFormat class explicitly specified nor StorageHandler class");
      }
      final HiveStorageHandler storageHandler = HiveUtils.getStorageHandler(job, storageHandlerClass);
      return (Class<? extends InputFormat<?, ?>>) storageHandler.getInputFormatClass();
    } else {
      return (Class<? extends InputFormat<?, ?>>) Class.forName(inputFormatName) ;
    }
  }

  /**
   * Utility method which adds give configs to {@link JobConf} object.
   *
   * @param job {@link JobConf} instance.
   * @param properties New config properties
   * @param hiveConf HiveConf of Hive storage plugin
   */
  public static void addConfToJob(final JobConf job, final Properties properties) {
    for (Object obj : properties.keySet()) {
      job.set((String) obj, (String) properties.get(obj));
    }
  }

  /**
   * Wrapper around {@link MetaStoreUtils#getPartitionMetadata(Partition, Table)} which also adds parameters from table
   * to properties returned by {@link MetaStoreUtils#getPartitionMetadata(Partition, Table)}.
   *
   * @param partition
   * @param table
   * @return
   */
  public static Properties getPartitionMetadata(final Partition partition, final Table table) {
    final Properties properties = MetaStoreUtils.getPartitionMetadata(partition, table);

    // SerDe expects properties from Table, but above call doesn't add Table properties.
    // Include Table properties in final list in order to not to break SerDes that depend on
    // Table properties. For example AvroSerDe gets the schema from properties (passed as second argument)
    for (Map.Entry<String, String> entry : table.getParameters().entrySet()) {
      if (entry.getKey() != null && entry.getKey() != null) {
        properties.put(entry.getKey(), entry.getValue());
      }
    }

    return properties;
  }

  public static void throwUnsupportedHiveDataTypeError(String unsupportedType) {
    StringBuilder errMsg = new StringBuilder();
    errMsg.append(String.format("Unsupported Hive data type %s. ", unsupportedType));
    errMsg.append(System.getProperty("line.separator"));
    errMsg.append("Following Hive data types are supported in Drill for querying: ");
    errMsg.append(
        "BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DATE, TIMESTAMP, BINARY, DECIMAL, STRING, VARCHAR and CHAR");

    throw UserException.unsupportedError()
        .message(errMsg.toString())
        .build(logger);
  }
}

