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
package org.apache.drill.exec.physical.impl.scan.convert;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.ValueWriter;

/**
 * Factory for standard conversions as outlined in the package header.
 * Can be used in two ways:
 * <ul>
 * <li>As an object with a default set of properties that apply to
 * all columns.</li>
 * <li>As a set of static functions which operate on each column
 * individually.</li>
 * </ul>
 * The object form is more typical: it allows the provided schema to
 * set general blank-handling behavior at the tuple level.
 * <p>
 * The class provides two kinds of information:
 * <p>
 * <ul>
 * <li>A description of the conversion: whether the conversion is supported,
 * whether it is lossy, and the class that can do the conversion.</li>
 * <li>As a concrete conversion factory which provides the conversion class
 * if needed, the original column writer if no conversion is needed, or
 * {@code null} if the conversion is not supported.</li>
 * </ul>
 * <p>
 * This class is not suitable if a reader requires a source-specific
 * column converter or conversion rules. In that case, create a source-specific
 * conversion factory.
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

    public final ConversionType type;
    public final Class<? extends DirectConverter> conversionClass;

    public ConversionDefn(ConversionType type) {
      this.type = type;
      conversionClass = null;
    }

    public ConversionDefn(Class<? extends DirectConverter> conversionClass) {
      this.type = ConversionType.EXPLICIT;
      this.conversionClass = conversionClass;
    }
  }

  public static final ConversionDefn IMPLICIT =
      new ConversionDefn(ConversionType.IMPLICIT);
  public static final ConversionDefn IMPLICIT_UNSAFE =
      new ConversionDefn(ConversionType.IMPLICIT_UNSAFE);
  public static final ConversionDefn EXPLICIT =
      new ConversionDefn(ConversionType.EXPLICIT);

  private final Map<String, String> properties;

  public StandardConversions() {
    this.properties = null;
  }

  public StandardConversions(Map<String,String> properties) {
    this.properties = properties;
  }

  public StandardConversions(String blankAsProp) {
    if (blankAsProp == null) {
      this.properties = null;
    } else {
      this.properties = new HashMap<>();
      this.properties.put(ColumnMetadata.BLANK_AS_PROP, blankAsProp);
    }
  }

  public StandardConversions(TupleMetadata providedSchema) {
    if (providedSchema == null || !providedSchema.hasProperties()) {
      this.properties = null;
    } else {
      this.properties = providedSchema.properties();
    }
  }

  private Map<String,String> merge(Map<String,String> specificProps) {
    if (properties == null) {
      return specificProps;
    } else if (specificProps == null) {
      return properties;
    }
    Map<String,String> merged = new HashMap<>();
    merged.putAll(properties);
    merged.putAll(specificProps);
    return merged;
  }

  public static DirectConverter newInstance(
      Class<? extends DirectConverter> conversionClass, ScalarWriter baseWriter,
      Map<String,String> properties) {

    // Try the Converter(ScalerWriter writer, Map<String, String> props) constructor first.
    // This first form is optional.
    try {
      final Constructor<? extends DirectConverter> ctor = conversionClass.getDeclaredConstructor(ScalarWriter.class, Map.class);
      return ctor.newInstance(baseWriter, properties);
    } catch (final ReflectiveOperationException e) {
      // Ignore
    }

    // Then try the Converter(ScalarSriter writer) constructor.
    return newInstance(conversionClass, baseWriter);
  }

  public static DirectConverter newInstance(
      Class<? extends DirectConverter> conversionClass, ScalarWriter baseWriter) {
    try {
      final Constructor<? extends DirectConverter> ctor = conversionClass.getDeclaredConstructor(ScalarWriter.class);
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
    return analyze(inputSchema.type(), outputSchema);
  }

  public static ConversionDefn analyze(MinorType inputType, ColumnMetadata outputSchema) {
    if (inputType == outputSchema.type()) {
      return new ConversionDefn(ConversionType.NONE);
    }

    switch (inputType) {
      case VARCHAR:
        return new ConversionDefn(convertFromVarchar(outputSchema));
      case BIT:
        switch (outputSchema.type()) {
        case TINYINT:
        case SMALLINT:
        case INT:
          return IMPLICIT;
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
          return IMPLICIT;
        case VARCHAR:
          return new ConversionDefn(ConvertIntToString.class);
        default:
          break;
        }
        break;
      case SMALLINT:
        switch (outputSchema.type()) {
        case TINYINT:
          return IMPLICIT_UNSAFE;
        case INT:
        case BIGINT:
        case FLOAT4:
        case FLOAT8:
        case VARDECIMAL:
          return IMPLICIT;
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
          return IMPLICIT_UNSAFE;
        case BIGINT:
        case FLOAT4:
        case FLOAT8:
        case VARDECIMAL:
        case TIME:
          return IMPLICIT;
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
          return IMPLICIT_UNSAFE;
        case FLOAT4:
        case FLOAT8:
        case VARDECIMAL:
        case DATE:
        case TIMESTAMP:
          return IMPLICIT;
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
          return IMPLICIT_UNSAFE;
        case FLOAT8:
        case VARDECIMAL:
          return IMPLICIT;
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
          return IMPLICIT_UNSAFE;
        case VARDECIMAL:
          return IMPLICIT;
        case VARCHAR:
          return new ConversionDefn(ConvertDoubleToString.class);
        default:
          break;
        }
        break;
      case DATE:
        switch (outputSchema.type()) {
        case BIGINT:
          return IMPLICIT;
        case VARCHAR:
          return new ConversionDefn(ConvertDateToString.class);
        default:
          break;
        }
        break;
      case TIME:
        switch (outputSchema.type()) {
        case INT:
          return IMPLICIT;
        case VARCHAR:
          return new ConversionDefn(ConvertTimeToString.class);
        default:
          break;
        }
        break;
      case TIMESTAMP:
        switch (outputSchema.type()) {
        case BIGINT:
          return IMPLICIT;
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
    return EXPLICIT;
  }

  public static Class<? extends DirectConverter> convertFromVarchar(
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

  /**
   * Create a direct column converter, if necessary, for the given input type
   * and optional properties. The properties provide formats and other type
   * conversion hints needed for some conversions.
   *
   * @param scalarWriter the output column writer
   * @param inputType the type of the input data
   * @param properties optional properties for some string-based conversions
   * @return a column converter, if needed and available, the input writer if
   * no conversion is needed, or null if there is no conversion available
   */
  public static ValueWriter converterFor(ScalarWriter scalarWriter, MinorType inputType, Map<String, String> properties) {
    ConversionDefn defn = analyze(inputType, scalarWriter.schema());
    switch (defn.type) {
      case EXPLICIT:
        if (defn.conversionClass != null) {
          return newInstance(defn.conversionClass, scalarWriter, properties);
        } else {
          return null;
        }
      case IMPLICIT:
      case IMPLICIT_UNSAFE:
        return newInstance(defn.conversionClass, scalarWriter, properties);
      default:
        return scalarWriter;
    }
  }

  public static ValueWriter converterFor(ScalarWriter scalarWriter, MinorType inputType) {
    return converterFor(scalarWriter, inputType, null);
  }

  public static ValueWriter converterFor(ScalarWriter scalarWriter, ColumnMetadata inputSchema) {
    return converterFor(scalarWriter, inputSchema.type(), inputSchema.properties());
  }

  public ValueWriter converter(ScalarWriter scalarWriter, ColumnMetadata inputSchema) {
    return converterFor(scalarWriter, inputSchema.type(),
        merge(inputSchema.properties()));
  }

  public ValueWriter converter(ScalarWriter scalarWriter, MinorType inputType) {
    return converterFor(scalarWriter, inputType, properties);
  }

  /**
   * Given a desired provided schema and an actual reader schema, create a merged
   * schema that contains the provided column where available, but the reader
   * column otherwise. Copies provided properties to the output schema.
   * <p>
   * The result is the schema to use when creating column writers: it reflects
   * the type of the target vector. The reader is responsible for converting from
   * the (possibly different) reader column type to the provided column type.
   * <p>
   * Note: the provided schema should only contain types that the reader is prepared
   * to offer: there is no requirement that the reader support every possible conversion,
   * only those that make sense for that one reader.
   *
   * @param providedSchema the provided schema from {@code CREATE SCHEMA}
   * @param readerSchema the set of column types that the reader can provide
   * "natively"
   * @return a merged schema to use when creating the {@code ResultSetLoader}
   */
  public static TupleMetadata mergeSchemas(TupleMetadata providedSchema,
      TupleMetadata readerSchema) {
    if (providedSchema == null) {
      return readerSchema;
    }
    final TupleMetadata tableSchema = new TupleSchema();
    for (ColumnMetadata readerCol : readerSchema) {
      final ColumnMetadata providedCol = providedSchema.metadata(readerCol.name());
      tableSchema.addColumn(providedCol == null ? readerCol : providedCol);
    }
    if (providedSchema.hasProperties()) {
      tableSchema.properties().putAll(providedSchema.properties());
    }
    return tableSchema;
  }
}
