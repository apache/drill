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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.ListenableFuture;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.collect.Lists;
import org.apache.hadoop.security.UserGroupInformation;

public class HiveRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveRecordReader.class);

  private final DrillBuf managedBuffer;

  protected Table table;
  protected Partition partition;
  protected InputSplit inputSplit;
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
  private String defaultPartitionValue;
  private final UserGroupInformation proxyUgi;

  protected static final int TARGET_RECORD_COUNT = 4000;

  public HiveRecordReader(Table table, Partition partition, InputSplit inputSplit, List<SchemaPath> projectedColumns,
                          FragmentContext context, Map<String, String> hiveConfigOverride,
                          UserGroupInformation proxyUgi) throws ExecutionSetupException {
    this.table = table;
    this.partition = partition;
    this.inputSplit = inputSplit;
    this.empty = (inputSplit == null && partition == null);
    this.hiveConfigOverride = hiveConfigOverride;
    this.fragmentContext = context;
    this.proxyUgi = proxyUgi;
    this.managedBuffer = fragmentContext.getManagedBuffer().reallocIfNeeded(256);
    setColumns(projectedColumns);
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

    // Get the configured default val
    defaultPartitionValue = HiveUtilities.getDefaultPartitionValue(hiveConfigOverride);

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
            selectedPartitionValues.add(
                HiveUtilities.convertPartitionType(pType, partition.getValues().get(i), defaultPartitionValue));
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
  public void setup(@SuppressWarnings("unused") OperatorContext context, OutputMutator output)
      throws ExecutionSetupException {
    // initializes "reader"
    final Callable<Void> readerInitializer = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        init();
        return null;
      }
    };

    final ListenableFuture<Void> result = context.runCallableAs(proxyUgi, readerInitializer);
    try {
      result.get();
    } catch (InterruptedException e) {
      result.cancel(true);
      // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      throw ExecutionSetupException.fromThrowable(e.getMessage(), e);
    }
    try {
      final OptionManager options = fragmentContext.getOptions();
      for (int i = 0; i < selectedColumnNames.size(); i++) {
        MajorType type = HiveUtilities.getMajorTypeFromHiveTypeInfo(selectedColumnTypes.get(i), options);
        MaterializedField field = MaterializedField.create(SchemaPath.getSimplePath(selectedColumnNames.get(i)), type);
        Class vvClass = TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode());
        vectors.add(output.addField(field, vvClass));
      }

      for (int i = 0; i < selectedPartitionNames.size(); i++) {
        MajorType type = HiveUtilities.getMajorTypeFromHiveTypeInfo(selectedPartitionTypes.get(i), options);
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

  protected void populatePartitionVectors(int recordCount) {
    for (int i = 0; i < pVectors.size(); i++) {
      final ValueVector vector = pVectors.get(i);
      final Object val = selectedPartitionValues.get(i);

      AllocationHelper.allocateNew(vector, recordCount);

      if (val != null) {
        HiveUtilities.populateVector(vector, managedBuffer, val, 0, recordCount);
      }

      vector.getMutator().setValueCount(recordCount);
    }
  }
}
