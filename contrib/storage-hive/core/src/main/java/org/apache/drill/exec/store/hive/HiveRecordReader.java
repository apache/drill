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

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.util.DecimalUtility;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.DateVector;
import org.apache.drill.exec.vector.Decimal18Vector;
import org.apache.drill.exec.vector.Decimal28SparseVector;
import org.apache.drill.exec.vector.Decimal38SparseVector;
import org.apache.drill.exec.vector.Decimal9Vector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.SmallIntVector;
import org.apache.drill.exec.vector.TimeStampVector;
import org.apache.drill.exec.vector.TinyIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.work.ExecErrorConstants;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.collect.Lists;

public class HiveRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveRecordReader.class);

  protected Table table;
  protected Partition partition;
  protected InputSplit inputSplit;
  protected FragmentContext context;
  protected List<String> selectedColumnNames;
  protected List<TypeInfo> selectedColumnTypes = Lists.newArrayList();
  protected List<ObjectInspector> selectedColumnObjInspectors = Lists.newArrayList();
  protected List<HiveFieldConverter> selectedColumnFieldConverters = Lists.newArrayList();
  protected List<String> selectedPartitionNames = Lists.newArrayList();
  protected List<TypeInfo> selectedPartitionTypes = Lists.newArrayList();
  protected List<Object> selectedPartitionValues = Lists.newArrayList();
  protected List<String> tableColumns; // all columns in table (not including partition columns)
  protected SerDe serde;
  protected StructObjectInspector sInspector;
  protected Object key, value;
  protected org.apache.hadoop.mapred.RecordReader reader;
  protected List<ValueVector> vectors = Lists.newArrayList();
  protected List<ValueVector> pVectors = Lists.newArrayList();
  protected Object redoRecord;
  protected boolean empty;
  private Map<String, String> hiveConfigOverride;
  private FragmentContext fragmentContext;
  private OperatorContext operatorContext;


  protected static final int TARGET_RECORD_COUNT = 4000;
  protected static final int FIELD_SIZE = 50;

  public HiveRecordReader(Table table, Partition partition, InputSplit inputSplit, List<SchemaPath> projectedColumns,
      FragmentContext context, Map<String, String> hiveConfigOverride) throws ExecutionSetupException {
    this.table = table;
    this.partition = partition;
    this.inputSplit = inputSplit;
    this.context = context;
    this.empty = (inputSplit == null && partition == null);
    this.hiveConfigOverride = hiveConfigOverride;
    this.fragmentContext=context;
    setColumns(projectedColumns);
    init();
  }

  private void init() throws ExecutionSetupException {
    Properties properties;
    JobConf job = new JobConf();
    if (partition != null) {
      properties = MetaStoreUtils.getPartitionMetadata(partition, table);

      // SerDe expects properties from Table, but above call doesn't add Table properties.
      // Include Table properties in final list in order to not to break SerDes that depend on
      // Table properties. For example AvroSerDe gets the schema from properties (passed as second argument)
      for (Map.Entry<String, String> entry : table.getParameters().entrySet()) {
        if (entry.getKey() != null && entry.getKey() != null) {
          properties.put(entry.getKey(), entry.getValue());
        }
      }
    } else {
      properties = MetaStoreUtils.getTableMetadata(table);
    }
    for (Object obj : properties.keySet()) {
      job.set((String) obj, (String) properties.get(obj));
    }
    for(Map.Entry<String, String> entry : hiveConfigOverride.entrySet()) {
      job.set(entry.getKey(), entry.getValue());
    }
    InputFormat format;
    String sLib = (partition == null) ? table.getSd().getSerdeInfo().getSerializationLib() : partition.getSd().getSerdeInfo().getSerializationLib();
    String inputFormatName = (partition == null) ? table.getSd().getInputFormat() : partition.getSd().getInputFormat();
    try {
      format = (InputFormat) Class.forName(inputFormatName).getConstructor().newInstance();
      Class<?> c = Class.forName(sLib);
      serde = (SerDe) c.getConstructor().newInstance();
      serde.initialize(job, properties);
    } catch (ReflectiveOperationException | SerDeException e) {
      throw new ExecutionSetupException("Unable to instantiate InputFormat", e);
    }
    job.setInputFormat(format.getClass());

    List<FieldSchema> partitionKeys = table.getPartitionKeys();
    List<String> partitionNames = Lists.newArrayList();
    for (FieldSchema field : partitionKeys) {
      partitionNames.add(field.getName());
    }

    try {
      ObjectInspector oi = serde.getObjectInspector();
      if (oi.getCategory() != ObjectInspector.Category.STRUCT) {
        throw new UnsupportedOperationException(String.format("%s category not supported", oi.getCategory()));
      }
      sInspector = (StructObjectInspector) oi;
      StructTypeInfo sTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(sInspector);
      List<Integer> columnIds = Lists.newArrayList();
      if (isStarQuery()) {
        selectedColumnNames = sTypeInfo.getAllStructFieldNames();
        tableColumns = selectedColumnNames;
        for(int i=0; i<selectedColumnNames.size(); i++) {
          columnIds.add(i);
        }
      } else {
        tableColumns = sTypeInfo.getAllStructFieldNames();
        selectedColumnNames = Lists.newArrayList();
        for (SchemaPath field : getColumns()) {
          String columnName = field.getRootSegment().getPath();
          if (!tableColumns.contains(columnName)) {
            if (partitionNames.contains(columnName)) {
              selectedPartitionNames.add(columnName);
            } else {
              throw new ExecutionSetupException(String.format("Column %s does not exist", columnName));
            }
          } else {
            columnIds.add(tableColumns.indexOf(columnName));
            selectedColumnNames.add(columnName);
          }
        }
      }
      ColumnProjectionUtils.appendReadColumns(job, columnIds, selectedColumnNames);

      for (String columnName : selectedColumnNames) {
        ObjectInspector fieldOI = sInspector.getStructFieldRef(columnName).getFieldObjectInspector();
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(fieldOI.getTypeName());

        selectedColumnObjInspectors.add(fieldOI);
        selectedColumnTypes.add(typeInfo);
        selectedColumnFieldConverters.add(HiveFieldConverter.create(typeInfo, fragmentContext));
      }

      if (isStarQuery()) {
        selectedPartitionNames = partitionNames;
      }

      for (int i = 0; i < table.getPartitionKeys().size(); i++) {
        FieldSchema field = table.getPartitionKeys().get(i);
        if (selectedPartitionNames.contains(field.getName())) {
          TypeInfo pType = TypeInfoUtils.getTypeInfoFromTypeString(field.getType());
          selectedPartitionTypes.add(pType);

          if (partition != null) {
            selectedPartitionValues.add(convertPartitionType(pType, partition.getValues().get(i)));
          }
        }
      }
    } catch (Exception e) {
      throw new ExecutionSetupException("Failure while initializing HiveRecordReader: " + e.getMessage(), e);
    }

    if (!empty) {
      try {
        reader = format.getRecordReader(inputSplit, job, Reporter.NULL);
      } catch (IOException e) {
        throw new ExecutionSetupException("Failed to get o.a.hadoop.mapred.RecordReader from Hive InputFormat", e);
      }
      key = reader.createKey();
      value = reader.createValue();
    }
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.operatorContext = context;
    try {
      for (int i = 0; i < selectedColumnNames.size(); i++) {
        MajorType type = getMajorTypeFromHiveTypeInfo(selectedColumnTypes.get(i), true);
        MaterializedField field = MaterializedField.create(SchemaPath.getSimplePath(selectedColumnNames.get(i)), type);
        Class vvClass = TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode());
        vectors.add(output.addField(field, vvClass));
      }

      for (int i = 0; i < selectedPartitionNames.size(); i++) {
        MajorType type = getMajorTypeFromHiveTypeInfo(selectedPartitionTypes.get(i), false);
        MaterializedField field = MaterializedField.create(SchemaPath.getSimplePath(selectedPartitionNames.get(i)), type);
        Class vvClass = TypeHelper.getValueVectorClass(field.getType().getMinorType(), field.getDataMode());
        pVectors.add(output.addField(field, vvClass));
      }
    } catch(SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    for (ValueVector vv : vectors) {
      AllocationHelper.allocateNew(vv, TARGET_RECORD_COUNT);
    }
    if (empty) {
      setValueCountAndPopulatePartitionVectors(0);
      return 0;
    }

    try {
      int recordCount = 0;

      if (redoRecord != null) {
        // Try writing the record that didn't fit into the last RecordBatch
        Object deSerializedValue = serde.deserialize((Writable) redoRecord);
        boolean status = readHiveRecordAndInsertIntoRecordBatch(deSerializedValue, recordCount);
        if (!status) {
          throw new DrillRuntimeException("Current record is too big to fit into allocated ValueVector buffer");
        }
        redoRecord = null;
        recordCount++;
      }

      while (recordCount < TARGET_RECORD_COUNT && reader.next(key, value)) {
        Object deSerializedValue = serde.deserialize((Writable) value);
        boolean status = readHiveRecordAndInsertIntoRecordBatch(deSerializedValue, recordCount);
        if (!status) {
          redoRecord = value;
          setValueCountAndPopulatePartitionVectors(recordCount);
          return recordCount;
        }
        recordCount++;
      }

      setValueCountAndPopulatePartitionVectors(recordCount);
      return recordCount;
    } catch (IOException | SerDeException e) {
      throw new DrillRuntimeException(e);
    }
  }

  private boolean readHiveRecordAndInsertIntoRecordBatch(Object deSerializedValue, int outputRecordIndex) {
    for (int i = 0; i < selectedColumnNames.size(); i++) {
      String columnName = selectedColumnNames.get(i);
      Object hiveValue = sInspector.getStructFieldData(deSerializedValue, sInspector.getStructFieldRef(columnName));

      if (hiveValue != null) {
        selectedColumnFieldConverters.get(i).setSafeValue(selectedColumnObjInspectors.get(i), hiveValue,
            vectors.get(i), outputRecordIndex);
      }
    }

    return true;
  }

  private void setValueCountAndPopulatePartitionVectors(int recordCount) {
    for (ValueVector v : vectors) {
      v.getMutator().setValueCount(recordCount);
    }

    if (partition != null) {
      populatePartitionVectors(recordCount);
    }
  }

  @Override
  public void close() {
    try {
      if (reader != null) {
        reader.close();
        reader = null;
      }
    } catch (Exception e) {
      logger.warn("Failure while closing Hive Record reader.", e);
    }
  }

  private MinorType getMinorTypeFromHivePrimitiveTypeInfo(PrimitiveTypeInfo primitiveTypeInfo) {
    switch(primitiveTypeInfo.getPrimitiveCategory()) {
      case BINARY:
        return TypeProtos.MinorType.VARBINARY;
      case BOOLEAN:
        return MinorType.BIT;
      case DECIMAL: {

        if (context.getOptions().getOption(PlannerSettings.ENABLE_DECIMAL_DATA_TYPE_KEY).bool_val == false) {
          throw UserException.unsupportedError()
              .message(ExecErrorConstants.DECIMAL_DISABLE_ERR_MSG)
              .build(logger);
        }
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
        return DecimalUtility.getDecimalDataType(decimalTypeInfo.precision());
      }
      case DOUBLE:
        return MinorType.FLOAT8;
      case FLOAT:
        return MinorType.FLOAT4;
      // TODO (DRILL-2470)
      // Byte and short (tinyint and smallint in SQL types) are currently read as integers
      // as these smaller integer types are not fully supported in Drill today.
      case SHORT:
      case BYTE:
      case INT:
        return MinorType.INT;
      case LONG:
        return MinorType.BIGINT;
      case STRING:
      case VARCHAR:
        return MinorType.VARCHAR;
      case TIMESTAMP:
        return MinorType.TIMESTAMP;
      case DATE:
        return MinorType.DATE;
    }

    throwUnsupportedHiveDataTypeError(primitiveTypeInfo.getPrimitiveCategory().toString());
    return null;
  }

  public MajorType getMajorTypeFromHiveTypeInfo(TypeInfo typeInfo, boolean nullable) {
    switch (typeInfo.getCategory()) {
      case PRIMITIVE: {
        PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        MinorType minorType = getMinorTypeFromHivePrimitiveTypeInfo(primitiveTypeInfo);
        MajorType.Builder typeBuilder = MajorType.newBuilder().setMinorType(minorType)
            .setMode((nullable ? DataMode.OPTIONAL : DataMode.REQUIRED));

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

  protected void populatePartitionVectors(int recordCount) {
    for (int i = 0; i < pVectors.size(); i++) {
      int size = 50;
      ValueVector vector = pVectors.get(i);
      Object val = selectedPartitionValues.get(i);
      PrimitiveCategory pCat = ((PrimitiveTypeInfo)selectedPartitionTypes.get(i)).getPrimitiveCategory();
      if (pCat == PrimitiveCategory.BINARY || pCat == PrimitiveCategory.STRING || pCat == PrimitiveCategory.VARCHAR) {
        size = ((byte[]) selectedPartitionValues.get(i)).length;
      }

      AllocationHelper.allocateNew(vector, recordCount);

      switch(pCat) {
        case BINARY: {
          VarBinaryVector v = (VarBinaryVector) vector;
          byte[] value = (byte[]) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case BOOLEAN: {
          BitVector v = (BitVector) vector;
          Boolean value = (Boolean) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().set(j, value ? 1 : 0);
          }
          break;
        }
        case DOUBLE: {
          Float8Vector v = (Float8Vector) vector;
          double value = (double) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case FLOAT: {
          Float4Vector v = (Float4Vector) vector;
          float value = (float) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case BYTE:
        case SHORT:
        case INT: {
          IntVector v = (IntVector) vector;
          int value = (int) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case LONG: {
          BigIntVector v = (BigIntVector) vector;
          long value = (long) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case VARCHAR:
        case STRING: {
          VarCharVector v = (VarCharVector) vector;
          byte[] value = (byte[]) val;
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case TIMESTAMP: {
          TimeStampVector v = (TimeStampVector) vector;
          DateTime ts = new DateTime(((Timestamp) val).getTime()).withZoneRetainFields(DateTimeZone.UTC);
          long value = ts.getMillis();
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case DATE: {
          DateVector v = (DateVector) vector;
          DateTime date = new DateTime(((Date)val).getTime()).withZoneRetainFields(DateTimeZone.UTC);
          long value = date.getMillis();
          for (int j = 0; j < recordCount; j++) {
            v.getMutator().setSafe(j, value);
          }
          break;
        }
        case DECIMAL: {
          populateDecimalPartitionVector((DecimalTypeInfo)selectedPartitionTypes.get(i), vector,
              ((HiveDecimal)val).bigDecimalValue(), recordCount);
          break;
        }
        default:
          throwUnsupportedHiveDataTypeError(pCat.toString());
      }
      vector.getMutator().setValueCount(recordCount);
    }
  }

  private void populateDecimalPartitionVector(DecimalTypeInfo typeInfo, ValueVector vector, BigDecimal bigDecimal,
      int recordCount) {
    int precision = typeInfo.precision();
    int scale = typeInfo.scale();
    if (precision <= 9) {
      Decimal9Holder holder = new Decimal9Holder();
      holder.scale = scale;
      holder.precision = precision;
      holder.value = DecimalUtility.getDecimal9FromBigDecimal(bigDecimal, scale, precision);
      Decimal9Vector v = (Decimal9Vector) vector;
      for (int j = 0; j < recordCount; j++) {
        v.getMutator().setSafe(j, holder);
      }
    } else if (precision <= 18) {
      Decimal18Holder holder = new Decimal18Holder();
      holder.scale = scale;
      holder.precision = precision;
      holder.value = DecimalUtility.getDecimal18FromBigDecimal(bigDecimal, scale, precision);
      Decimal18Vector v = (Decimal18Vector) vector;
      for (int j = 0; j < recordCount; j++) {
        v.getMutator().setSafe(j, holder);
      }
    } else if (precision <= 28) {
      Decimal28SparseHolder holder = new Decimal28SparseHolder();
      holder.scale = scale;
      holder.precision = precision;
      holder.buffer = fragmentContext.getManagedBuffer(
          Decimal28SparseHolder.nDecimalDigits * DecimalUtility.integerSize);
      holder.start = 0;
      DecimalUtility.getSparseFromBigDecimal(bigDecimal, holder.buffer, 0, scale, precision,
          Decimal28SparseHolder.nDecimalDigits);
      Decimal28SparseVector v = (Decimal28SparseVector) vector;
      for (int j = 0; j < recordCount; j++) {
        v.getMutator().setSafe(j, holder);
      }
    } else {
      Decimal38SparseHolder holder = new Decimal38SparseHolder();
      holder.scale = scale;
      holder.precision = precision;
      holder.buffer = fragmentContext.getManagedBuffer(
          Decimal38SparseHolder.nDecimalDigits * DecimalUtility.integerSize);
      holder.start = 0;
      DecimalUtility.getSparseFromBigDecimal(bigDecimal, holder.buffer, 0, scale, precision,
          Decimal38SparseHolder.nDecimalDigits);
      Decimal38SparseVector v = (Decimal38SparseVector) vector;
      for (int j = 0; j < recordCount; j++) {
        v.getMutator().setSafe(j, holder);
      }
    }
  }

  /** Partition value is received in string format. Convert it into appropriate object based on the type. */
  private Object convertPartitionType(TypeInfo typeInfo, String value) {
    if (typeInfo.getCategory() != Category.PRIMITIVE) {
      // In Hive only primitive types are allowed as partition column types.
      throw new DrillRuntimeException("Non-Primitive types are not allowed as partition column type in Hive, " +
          "but received one: " + typeInfo.getCategory());
    }

    PrimitiveCategory pCat = ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory();
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
      case TIMESTAMP:
        return Timestamp.valueOf(value);
      case DATE:
        return Date.valueOf(value);
    }

    throwUnsupportedHiveDataTypeError(pCat.toString());
    return null;
  }

  public static void throwUnsupportedHiveDataTypeError(String unsupportedType) {
    StringBuilder errMsg = new StringBuilder();
    errMsg.append(String.format("Unsupported Hive data type %s. ", unsupportedType));
    errMsg.append(System.getProperty("line.separator"));
    errMsg.append("Following Hive data types are supported in Drill for querying: ");
    errMsg.append(
        "BOOLEAN, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, TIMESTAMP, BINARY, DECIMAL, STRING, and VARCHAR");

    throw new RuntimeException(errMsg.toString());
  }
}
