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
package org.apache.drill.exec.store.hive.readers;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.drill.shaded.guava.com.google.common.util.concurrent.ListenableFuture;
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
import org.apache.drill.exec.store.hive.HiveFieldConverter;
import org.apache.drill.exec.store.hive.HivePartition;
import org.apache.drill.exec.store.hive.HiveTableWithColumnCache;
import org.apache.drill.exec.store.hive.HiveUtilities;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.hadoop.security.UserGroupInformation;


public abstract class HiveAbstractReader extends AbstractRecordReader {
  protected static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HiveAbstractReader.class);

  protected final DrillBuf managedBuffer;

  protected HiveTableWithColumnCache table;
  protected HivePartition partition;
  protected Iterator<InputSplit> inputSplitsIterator;
  protected List<String> selectedColumnNames;
  protected List<StructField> selectedStructFieldRefs = Lists.newArrayList();
  protected List<TypeInfo> selectedColumnTypes = Lists.newArrayList();
  protected List<ObjectInspector> selectedColumnObjInspectors = Lists.newArrayList();
  protected List<HiveFieldConverter> selectedColumnFieldConverters = Lists.newArrayList();
  protected List<String> selectedPartitionNames = Lists.newArrayList();
  protected List<TypeInfo> selectedPartitionTypes = Lists.newArrayList();
  protected List<Object> selectedPartitionValues = Lists.newArrayList();

  // Deserializer of the reading partition (or table if the table is non-partitioned)
  protected Deserializer partitionDeserializer;

  // ObjectInspector to read data from partitionDeserializer (for a non-partitioned table this is same as the table
  // ObjectInspector).
  protected StructObjectInspector partitionOI;

  // Final ObjectInspector. We may not use the partitionOI directly if there are schema changes between the table and
  // partition. If there are no schema changes then this is same as the partitionOI.
  protected StructObjectInspector finalOI;

  // Converter which converts data from partition schema to table schema.
  protected Converter partTblObjectInspectorConverter;

  protected Object key;
  protected RecordReader<Object, Object> reader;
  protected List<ValueVector> vectors = Lists.newArrayList();
  protected List<ValueVector> pVectors = Lists.newArrayList();
  protected boolean empty;
  protected HiveConf hiveConf;
  protected FragmentContext fragmentContext;
  protected String defaultPartitionValue;
  protected final UserGroupInformation proxyUgi;
  protected JobConf job;


  public static final int TARGET_RECORD_COUNT = 4000;

  public HiveAbstractReader(HiveTableWithColumnCache table, HivePartition partition, Collection<InputSplit> inputSplits, List<SchemaPath> projectedColumns,
                            FragmentContext context, final HiveConf hiveConf,
                            UserGroupInformation proxyUgi) throws ExecutionSetupException {
    this.table = table;
    this.partition = partition;
    this.empty = (inputSplits == null || inputSplits.isEmpty());
    this.inputSplitsIterator = empty ? Collections.<InputSplit>emptyIterator() : inputSplits.iterator();
    this.hiveConf = hiveConf;
    this.fragmentContext = context;
    this.proxyUgi = proxyUgi;
    this.managedBuffer = fragmentContext.getManagedBuffer().reallocIfNeeded(256);
    setColumns(projectedColumns);
  }

  public abstract void internalInit(Properties tableProperties, RecordReader<Object, Object> reader);

  private void init() throws ExecutionSetupException {
    job = new JobConf(hiveConf);

    // Get the configured default val
    defaultPartitionValue = hiveConf.get(ConfVars.DEFAULTPARTITIONNAME.varname);

    Properties tableProperties;
    try {
      tableProperties = HiveUtilities.getTableMetadata(table);
      final Properties partitionProperties =
          (partition == null) ?  tableProperties :
              HiveUtilities.getPartitionMetadata(partition, table);
      HiveUtilities.addConfToJob(job, partitionProperties);

      final Deserializer tableDeserializer = createDeserializer(job, table.getSd().getSerdeInfo().getSerializationLib(), tableProperties);
      final StructObjectInspector tableOI = getStructOI(tableDeserializer);

      if (partition != null) {
        partitionDeserializer = createDeserializer(job, partition.getSd().getSerdeInfo().getSerializationLib(), partitionProperties);
        partitionOI = getStructOI(partitionDeserializer);

        finalOI = (StructObjectInspector)ObjectInspectorConverters.getConvertedOI(partitionOI, tableOI);
        partTblObjectInspectorConverter = ObjectInspectorConverters.getConverter(partitionOI, finalOI);
        job.setInputFormat(HiveUtilities.getInputFormatClass(job, partition.getSd(), table));
        HiveUtilities.verifyAndAddTransactionalProperties(job, partition.getSd());
      } else {
        // For non-partitioned tables, there is no need to create converter as there are no schema changes expected.
        partitionDeserializer = tableDeserializer;
        partitionOI = tableOI;
        partTblObjectInspectorConverter = null;
        finalOI = tableOI;
        job.setInputFormat(HiveUtilities.getInputFormatClass(job, table.getSd(), table));
        HiveUtilities.verifyAndAddTransactionalProperties(job, table.getSd());
      }

      if (logger.isTraceEnabled()) {
        for (StructField field: finalOI.getAllStructFieldRefs()) {
          logger.trace("field in finalOI: {}", field.getClass().getName());
        }
        logger.trace("partitionDeserializer class is {} {}", partitionDeserializer.getClass().getName());
      }
      // Get list of partition column names
      final List<String> partitionNames = Lists.newArrayList();
      for (FieldSchema field : table.getPartitionKeys()) {
        partitionNames.add(field.getName());
      }

      // We should always get the columns names from ObjectInspector. For some of the tables (ex. avro) metastore
      // may not contain the schema, instead it is derived from other sources such as table properties or external file.
      // Deserializer object knows how to get the schema with all the config and table properties passed in initialization.
      // ObjectInspector created from the Deserializer object has the schema.
      final StructTypeInfo sTypeInfo = (StructTypeInfo) TypeInfoUtils.getTypeInfoFromObjectInspector(finalOI);
      final List<String> tableColumnNames = sTypeInfo.getAllStructFieldNames();

      // Select list of columns for project pushdown into Hive SerDe readers.
      final List<Integer> columnIds = Lists.newArrayList();
      if (isStarQuery()) {
        selectedColumnNames = tableColumnNames;
        for(int i=0; i<selectedColumnNames.size(); i++) {
          columnIds.add(i);
        }
        selectedPartitionNames = partitionNames;
      } else {
        selectedColumnNames = Lists.newArrayList();
        for (SchemaPath field : getColumns()) {
          String columnName = field.getRootSegment().getPath();
          if (partitionNames.contains(columnName)) {
            selectedPartitionNames.add(columnName);
          } else {
            columnIds.add(tableColumnNames.indexOf(columnName));
            selectedColumnNames.add(columnName);
          }
        }
      }
      List<String> paths = getColumns().stream()
          .map(SchemaPath::getRootSegmentPath)
          .collect(Collectors.toList());
      ColumnProjectionUtils.appendReadColumns(job, columnIds, selectedColumnNames, paths);

      for (String columnName : selectedColumnNames) {
        StructField fieldRef = finalOI.getStructFieldRef(columnName);
        selectedStructFieldRefs.add(fieldRef);
        ObjectInspector fieldOI = fieldRef.getFieldObjectInspector();

        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(fieldOI.getTypeName());

        selectedColumnObjInspectors.add(fieldOI);
        selectedColumnTypes.add(typeInfo);
        selectedColumnFieldConverters.add(HiveFieldConverter.create(typeInfo));
      }

      for(int i=0; i<selectedColumnNames.size(); ++i){
        logger.trace("inspector:typeName={}, className={}, TypeInfo: {}, converter:{}",
            selectedColumnObjInspectors.get(i).getTypeName(),
            selectedColumnObjInspectors.get(i).getClass().getName(),
            selectedColumnTypes.get(i).toString(),
            selectedColumnFieldConverters.get(i).getClass().getName());
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
      throw new ExecutionSetupException("Failure while initializing Hive Reader " + this.getClass().getName(), e);
    }

    if (!empty && initNextReader(job)) {
      internalInit(tableProperties, reader);
    }
  }

  /**
   * Initializes next reader if available, will close previous reader if any.
   *
   * @param job map / reduce job configuration.
   * @return true if new reader was initialized, false is no more readers are available
   * @throws ExecutionSetupException if could not init record reader
   */
  protected boolean initNextReader(JobConf job) throws ExecutionSetupException {
    if (inputSplitsIterator.hasNext()) {
      if (reader != null) {
        closeReader();
      }
      InputSplit inputSplit = inputSplitsIterator.next();
      try {
        reader = (org.apache.hadoop.mapred.RecordReader<Object, Object>) job.getInputFormat().getRecordReader(inputSplit, job, Reporter.NULL);
        logger.trace("hive reader created: {} for inputSplit {}", reader.getClass().getName(), inputSplit.toString());
      } catch (Exception e) {
        throw new ExecutionSetupException("Failed to get o.a.hadoop.mapred.RecordReader from Hive InputFormat", e);
      }
      return true;
    }
    return false;
  }

  /**
   * Utility method which creates a Deserializer object for given Deserializer class name and properties.
   * TODO: Replace Deserializer interface with AbstractSerDe, once all Hive clients is upgraded to 2.3 version
   */
  private static Deserializer createDeserializer(final JobConf job, final String sLib, final Properties properties) throws Exception {
    final Class<? extends Deserializer> c = Class.forName(sLib).asSubclass(Deserializer.class);
    final Deserializer deserializer = c.getConstructor().newInstance();
    deserializer.initialize(job, properties);

    return deserializer;
  }

  private static StructObjectInspector getStructOI(final Deserializer deserializer) throws Exception {
    ObjectInspector oi = deserializer.getObjectInspector();
    if (oi.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new UnsupportedOperationException(String.format("%s category not supported", oi.getCategory()));
    }
    return (StructObjectInspector) oi;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output)
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
        MaterializedField field = MaterializedField.create(selectedColumnNames.get(i), type);
        Class<? extends ValueVector> vvClass = TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode());
        vectors.add(output.addField(field, vvClass));
      }

      for (int i = 0; i < selectedPartitionNames.size(); i++) {
        MajorType type = HiveUtilities.getMajorTypeFromHiveTypeInfo(selectedPartitionTypes.get(i), options);
        MaterializedField field = MaterializedField.create(selectedPartitionNames.get(i), type);
        Class<? extends ValueVector> vvClass = TypeHelper.getValueVectorClass(field.getType().getMinorType(), field.getDataMode());
        pVectors.add(output.addField(field, vvClass));
      }
    } catch(SchemaChangeException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public abstract int next();

  protected void setValueCountAndPopulatePartitionVectors(int recordCount) {
    for (ValueVector v : vectors) {
      v.getMutator().setValueCount(recordCount);
    }

    if (partition != null) {
      populatePartitionVectors(recordCount);
    }
  }

  protected void readHiveRecordAndInsertIntoRecordBatch(Object deSerializedValue, int outputRecordIndex) {
    for (int i = 0; i < selectedStructFieldRefs.size(); i++) {
      Object hiveValue = finalOI.getStructFieldData(deSerializedValue, selectedStructFieldRefs.get(i));
      if (hiveValue != null) {
        selectedColumnFieldConverters.get(i).setSafeValue(selectedColumnObjInspectors.get(i), hiveValue,
            vectors.get(i), outputRecordIndex);
      }
    }
  }

  @Override
  public void close() {
    closeReader();
  }

  /**
   * Will close record reader if any. Any exception will be logged as warning.
   */
  private void closeReader() {
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

  /**
   * Writes value in the given value holder if next value available.
   * If value is not, checks if there are any other available readers
   * that may hold next value and tried to obtain value from them.
   *
   * @param value value holder
   * @return true if value was written, false otherwise
   */
  protected boolean hasNextValue(Object value) {
    while (true) {
      try {
        if (reader.next(key, value)) {
          return true;
        }

        if (initNextReader(job)) {
          continue;
        }

        return false;

      } catch (IOException | ExecutionSetupException e) {
        throw new DrillRuntimeException(e);
      }
    }
  }

  @Override
  public String toString() {
    long position = -1;
    try {
      if (reader != null) {
        position = reader.getPos();
      }
    } catch (IOException e) {
      logger.trace("Unable to obtain reader position.", e);
    }
    return getClass().getSimpleName() + "[Database=" + table.getDbName()
        + ", Table=" + table.getTableName()
        + ", Position=" + position
        + "]";
  }
}
