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
package org.apache.drill.exec.vector.accessor.convert;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ScalarWriter;

/**
 * Standard type conversion tools. Provides a mapping from input to output
 * (vector) type. Handles implicit conversions (those done by the column
 * writer itself) and explicit conversions for the Java string notation
 * for various types. Readers must provide custom conversions or specialized
 * formats.
 * <p>
 * Type-narrowing operations are supported by the column writers using
 * "Java semantics". Long-to-int overflow is caught, double-to-long conversion
 * sets the maximum or minimum long values, Double-to-int will overflow (on
 * the int conversion.) Messy, but the goal is not to handle invalid data,
 * rather it is to provide convenience for valid data.
 * <p>
 * The semantics of invalid conversions can be refined (set to null?
 * different exceptions) without affecting the behavior of queries with
 * valid data.
 */
public class StandardConversions {

  /**
   * Indicates the type of conversion needed.
   */
  public enum ConversionType {

    /**
     * No conversion needed. Readers generally don't provide converters
     * in this case unless the meaning of a column must change, keeping
     * the type, such as converting between units.
     */
    NONE,
    /**
     * Conversion is done by the column writers. Again, no converter
     * is needed except for semantic reasons.
     */
    IMPLICIT,
    /**
     * Conversion is done by the column writers. No converter is needed.
     * However, the value is subject to overflow as this is a narrowing
     * operation. Depending on the implementation, the operation may
     * either raise an error, or produce a value limited by the range
     * of the target type.
     */
    IMPLICIT_UNSAFE,
    /**
     * Conversion is needed because there is no "natural",
     * precision-preserving conversion. Conversions must be done on
     * an ad-hoc basis.
     */
    EXPLICIT
  }

  /**
   * Definition of a conversion including conversion type and the standard
   * conversion class (if available.)
   */
  public static class ConversionDefn {
    public static final ConversionDefn IMPLICIT =
        new ConversionDefn(ConversionType.IMPLICIT);
    public static final ConversionDefn IMPLICIT_UNSAFE =
        new ConversionDefn(ConversionType.IMPLICIT_UNSAFE);
    public static final ConversionDefn EXPLICIT =
        new ConversionDefn(ConversionType.EXPLICIT);

    public final ConversionType type;
    public final Class<? extends AbstractWriteConverter> conversionClass;

    public ConversionDefn(ConversionType type) {
      this.type = type;
      conversionClass = null;
    }

    public ConversionDefn(Class<? extends AbstractWriteConverter> conversionClass) {
      this.type = ConversionType.EXPLICIT;
      this.conversionClass = conversionClass;
    }
  }

  /**
   * Column conversion factory for the case where a conversion class is provided.
   * Also holds an optional set of properties to be passed to the converter instance.
   */
  public static class SimpleWriterConverterFactory implements ColumnConversionFactory {
    private final Class<? extends AbstractWriteConverter> conversionClass;
    private final Map<String, String> properties;

    SimpleWriterConverterFactory(Class<? extends AbstractWriteConverter> conversionClass,
        Map<String, String> properties) {
      this.conversionClass = conversionClass;
      this.properties = properties;
    }

    @Override
    public AbstractWriteConverter newWriter(ScalarWriter baseWriter) {
      return newInstance(conversionClass, baseWriter, properties);
    }
  }

  public static ColumnConversionFactory factory(Class<? extends AbstractWriteConverter> converterClass) {
    return new SimpleWriterConverterFactory(converterClass, null);
  }

  public static ColumnConversionFactory factory(Class<? extends AbstractWriteConverter> converterClass,
      Map<String, String> properties) {
    return new SimpleWriterConverterFactory(converterClass, properties);
  }

  public static AbstractWriteConverter newInstance(
      Class<? extends AbstractWriteConverter> conversionClass, ScalarWriter baseWriter,
      Map<String,String> properties) {

    // Try the Converter(ScalerWriter writer, Map<String, String> props) constructor first.
    // This first form is optional.

    try {
      final Constructor<? extends AbstractWriteConverter> ctor = conversionClass.getDeclaredConstructor(ScalarWriter.class, Map.class);
      return ctor.newInstance(baseWriter, properties);
    } catch (final ReflectiveOperationException e) {
      // Ignore
    }

    // Then try the Converter(ScalarSriter writer) constructor.

    return newInstance(conversionClass, baseWriter);
  }

  public static AbstractWriteConverter newInstance(
      Class<? extends AbstractWriteConverter> conversionClass, ScalarWriter baseWriter) {
    try {
      final Constructor<? extends AbstractWriteConverter> ctor = conversionClass.getDeclaredConstructor(ScalarWriter.class);
      return ctor.newInstance(baseWriter);
    } catch (final ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Create converters for standard cases.
   * <p>
   * Does not support any of the "legacy" decimal types.
   *
   * @param inputDefn the column schema for the input column which the
   * client code (e.g. reader) wants to produce
   * @param outputDefn the column schema for the output vector to be produced
   * by this operator
   * @return a description of the conversion needed (if any), along with the
   * standard conversion class, if available
   */
  public static ConversionDefn analyze(ColumnMetadata inputSchema, ColumnMetadata outputSchema) {
    if (inputSchema.type().equals(outputSchema.type())) {
      return new ConversionDefn(ConversionType.NONE);
    }

    switch (inputSchema.type()) {
    case VARCHAR:
      return new ConversionDefn(convertFromVarchar(outputSchema));
    case BIT:
      switch (outputSchema.type()) {
      case TINYINT:
      case SMALLINT:
      case INT:
        return ConversionDefn.IMPLICIT;
      case VARCHAR:
        return new ConversionDefn(ConvertBooleanToString.class);
      default:
        break;
      }
      break;
    case TINYINT:
      switch (outputSchema.type()) {
      case SMALLINT:
      case INT:
      case BIGINT:
      case FLOAT4:
      case FLOAT8:
      case VARDECIMAL:
        return ConversionDefn.IMPLICIT;
      case VARCHAR:
        return new ConversionDefn(ConvertIntToString.class);
      default:
        break;
      }
      break;
    case SMALLINT:
      switch (outputSchema.type()) {
      case TINYINT:
        return ConversionDefn.IMPLICIT_UNSAFE;
      case INT:
      case BIGINT:
      case FLOAT4:
      case FLOAT8:
      case VARDECIMAL:
        return ConversionDefn.IMPLICIT;
      case VARCHAR:
        return new ConversionDefn(ConvertIntToString.class);
     default:
        break;
      }
      break;
    case INT:
      switch (outputSchema.type()) {
      case TINYINT:
      case SMALLINT:
        return ConversionDefn.IMPLICIT_UNSAFE;
      case BIGINT:
      case FLOAT4:
      case FLOAT8:
      case VARDECIMAL:
      case TIME:
        return ConversionDefn.IMPLICIT;
      case VARCHAR:
        return new ConversionDefn(ConvertIntToString.class);
      default:
        break;
      }
      break;
    case BIGINT:
      switch (outputSchema.type()) {
      case TINYINT:
      case SMALLINT:
      case INT:
        return ConversionDefn.IMPLICIT_UNSAFE;
      case FLOAT4:
      case FLOAT8:
      case VARDECIMAL:
      case DATE:
      case TIMESTAMP:
        return ConversionDefn.IMPLICIT;
      case VARCHAR:
        return new ConversionDefn(ConvertLongToString.class);
      default:
        break;
      }
      break;
    case FLOAT4:
      switch (outputSchema.type()) {
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
        return ConversionDefn.IMPLICIT_UNSAFE;
      case FLOAT8:
      case VARDECIMAL:
        return ConversionDefn.IMPLICIT;
      case VARCHAR:
        return new ConversionDefn(ConvertDoubleToString.class);
      default:
        break;
      }
      break;
    case FLOAT8:
      switch (outputSchema.type()) {
      case TINYINT:
      case SMALLINT:
      case INT:
      case BIGINT:
      case FLOAT4:
        return ConversionDefn.IMPLICIT_UNSAFE;
      case VARDECIMAL:
        return ConversionDefn.IMPLICIT;
      case VARCHAR:
        return new ConversionDefn(ConvertDoubleToString.class);
      default:
        break;
      }
      break;
    case DATE:
      switch (outputSchema.type()) {
      case BIGINT:
        return ConversionDefn.IMPLICIT;
      case VARCHAR:
        return new ConversionDefn(ConvertDateToString.class);
      default:
        break;
      }
      break;
    case TIME:
      switch (outputSchema.type()) {
      case INT:
        return ConversionDefn.IMPLICIT;
      case VARCHAR:
        return new ConversionDefn(ConvertTimeToString.class);
      default:
        break;
      }
      break;
    case TIMESTAMP:
      switch (outputSchema.type()) {
      case BIGINT:
        return ConversionDefn.IMPLICIT;
      case VARCHAR:
        return new ConversionDefn(ConvertTimeStampToString.class);
      default:
        break;
      }
      break;
    case INTERVAL:
    case INTERVALYEAR:
    case INTERVALDAY:
      switch (outputSchema.type()) {
      case VARCHAR:
        return new ConversionDefn(ConvertIntervalToString.class);
      default:
        break;
      }
      break;
    default:
      break;
    }
    return ConversionDefn.EXPLICIT;
  }

  public static Class<? extends AbstractWriteConverter> convertFromVarchar(
      ColumnMetadata outputDefn) {
    switch (outputDefn.type()) {
    case BIT:
      return ConvertStringToBoolean.class;
    case TINYINT:
    case SMALLINT:
    case INT:
    case UINT1:
    case UINT2:
      return ConvertStringToInt.class;
    case BIGINT:
      return ConvertStringToLong.class;
    case FLOAT4:
    case FLOAT8:
      return ConvertStringToDouble.class;
    case DATE:
      return ConvertStringToDate.class;
    case TIME:
      return ConvertStringToTime.class;
    case TIMESTAMP:
      return ConvertStringToTimeStamp.class;
    case INTERVALYEAR:
    case INTERVALDAY:
    case INTERVAL:
      return ConvertStringToInterval.class;
    case VARDECIMAL:
      return ConvertStringToDecimal.class;
    default:
      return null;
    }
  }
}
