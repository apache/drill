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

/**
 * This template is used to generate different Hive record reader classes for different data formats
 * to avoid JIT profile pullusion. These readers are derived from HiveAbstractReader which implements
 * codes for init and setup stage, but the repeated - and performance critical part - next() method is
 * separately implemented in the classes generated from this template. The internal SkipRecordReeader
 * class is also separated as well due to the same reason.
 *
 * As to the performance gain with this change, please refer to:
 * https://issues.apache.org/jira/browse/DRILL-4982
 *
 */
<@pp.dropOutputFile />
<#list hiveFormat.map as entry>
<@pp.changeOutputFile name="/org/apache/drill/exec/store/hive/Hive${entry.hiveReader}Reader.java" />
<#include "/@includes/license.ftl" />

package org.apache.drill.exec.store.hive;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.hadoop.hive.serde2.SerDeException;

import org.apache.hadoop.mapred.RecordReader;
<#if entry.hasHeaderFooter == true>
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
</#if>

public class Hive${entry.hiveReader}Reader extends HiveAbstractReader {

  Object key;
<#if entry.hasHeaderFooter == true>
  SkipRecordsInspector skipRecordsInspector;
<#else>
  Object value;
</#if>

  public Hive${entry.hiveReader}Reader(HiveTableWithColumnCache table, HivePartition partition, InputSplit inputSplit, List<SchemaPath> projectedColumns,
                       FragmentContext context, final HiveConf hiveConf,
                       UserGroupInformation proxyUgi) throws ExecutionSetupException {
    super(table, partition, inputSplit, projectedColumns, context, hiveConf, proxyUgi);
  }

  public  void internalInit(Properties tableProperties, RecordReader<Object, Object> reader) {

    key = reader.createKey();
<#if entry.hasHeaderFooter == true>
    skipRecordsInspector = new SkipRecordsInspector(tableProperties, reader);
<#else>
    value = reader.createValue();
</#if>

  }
  private void readHiveRecordAndInsertIntoRecordBatch(Object deSerializedValue, int outputRecordIndex) {
    for (int i = 0; i < selectedStructFieldRefs.size(); i++) {
      Object hiveValue = finalOI.getStructFieldData(deSerializedValue, selectedStructFieldRefs.get(i));
      if (hiveValue != null) {
        selectedColumnFieldConverters.get(i).setSafeValue(selectedColumnObjInspectors.get(i), hiveValue,
          vectors.get(i), outputRecordIndex);
      }
    }
  }

<#if entry.hasHeaderFooter == true>
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
      skipRecordsInspector.reset();
      Object value;

      int recordCount = 0;

      while (recordCount < TARGET_RECORD_COUNT && reader.next(key, value = skipRecordsInspector.getNextValue())) {
        if (skipRecordsInspector.doSkipHeader(recordCount++)) {
          continue;
        }
        Object bufferedValue = skipRecordsInspector.bufferAdd(value);
        if (bufferedValue != null) {
          Object deSerializedValue = partitionSerDe.deserialize((Writable) bufferedValue);
          if (partTblObjectInspectorConverter != null) {
            deSerializedValue = partTblObjectInspectorConverter.convert(deSerializedValue);
          }
          readHiveRecordAndInsertIntoRecordBatch(deSerializedValue, skipRecordsInspector.getActualCount());
          skipRecordsInspector.incrementActualCount();
        }
        skipRecordsInspector.incrementTempCount();
      }

      setValueCountAndPopulatePartitionVectors(skipRecordsInspector.getActualCount());
      skipRecordsInspector.updateContinuance();
      return skipRecordsInspector.getActualCount();
    } catch (IOException | SerDeException e) {
      throw new DrillRuntimeException(e);
    }
  }

/**
 * SkipRecordsInspector encapsulates logic to skip header and footer from file.
 * Logic is applicable only for predefined in constructor file formats.
 */
protected class SkipRecordsInspector {

  private final Set<Object> fileFormats;
  private int headerCount;
  private int footerCount;
  private Queue<Object> footerBuffer;
  // indicates if we continue reading the same file
  private boolean continuance;
  private int holderIndex;
  private List<Object> valueHolder;
  private int actualCount;
  // actualCount without headerCount, used to determine holderIndex
  private int tempCount;

  protected SkipRecordsInspector(Properties tableProperties, RecordReader reader) {
    this.fileFormats = new HashSet<Object>(Arrays.asList(org.apache.hadoop.mapred.TextInputFormat.class.getName()));
    this.headerCount = retrievePositiveIntProperty(tableProperties, serdeConstants.HEADER_COUNT, 0);
    this.footerCount = retrievePositiveIntProperty(tableProperties, serdeConstants.FOOTER_COUNT, 0);
    logger.debug("skipRecordInspector: fileFormat {}, headerCount {}, footerCount {}",
        this.fileFormats, this.headerCount, this.footerCount);
    this.footerBuffer = Lists.newLinkedList();
    this.continuance = false;
    this.holderIndex = -1;
    this.valueHolder = initializeValueHolder(reader, footerCount);
    this.actualCount = 0;
    this.tempCount = 0;
  }

  protected boolean doSkipHeader(int recordCount) {
    return !continuance && recordCount < headerCount;
  }

  protected void reset() {
    tempCount = holderIndex + 1;
    actualCount = 0;
    if (!continuance) {
      footerBuffer.clear();
    }
  }

  protected Object bufferAdd(Object value) throws SerDeException {
    footerBuffer.add(value);
    if (footerBuffer.size() <= footerCount) {
      return null;
    }
    return footerBuffer.poll();
  }

  protected Object getNextValue() {
    holderIndex = tempCount % getHolderSize();
    return valueHolder.get(holderIndex);
  }

  private int getHolderSize() {
    return valueHolder.size();
  }

  protected void updateContinuance() {
    this.continuance = actualCount != 0;
  }

  protected int incrementTempCount() {
    return ++tempCount;
  }

  protected int getActualCount() {
    return actualCount;
  }

  protected int incrementActualCount() {
    return ++actualCount;
  }

  /**
   * Retrieves positive numeric property from Properties object by name.
   * Return default value if
   * 1. file format is absent in predefined file formats list
   * 2. property doesn't exist in table properties
   * 3. property value is negative
   * otherwise casts value to int.
   *
   * @param tableProperties property holder
   * @param propertyName    name of the property
   * @param defaultValue    default value
   * @return property numeric value
   * @throws NumberFormatException if property value is non-numeric
   */
  protected int retrievePositiveIntProperty(Properties tableProperties, String propertyName, int defaultValue) {
    int propertyIntValue = defaultValue;
    if (!fileFormats.contains(tableProperties.get(hive_metastoreConstants.FILE_INPUT_FORMAT))) {
      return propertyIntValue;
    }
    Object propertyObject = tableProperties.get(propertyName);
    if (propertyObject != null) {
      try {
        propertyIntValue = Integer.valueOf((String) propertyObject);
      } catch (NumberFormatException e) {
        throw new NumberFormatException(String.format("Hive table property %s value '%s' is non-numeric", propertyName, propertyObject.toString()));
      }
    }
    return propertyIntValue < 0 ? defaultValue : propertyIntValue;
  }

  /**
   * Creates buffer of objects to be used as values, so these values can be re-used.
   * Objects number depends on number of lines to skip in the end of the file plus one object.
   *
   * @param reader          RecordReader to return value object
   * @param skipFooterLines number of lines to skip at the end of the file
   * @return list of objects to be used as values
   */
  private List<Object> initializeValueHolder(RecordReader reader, int skipFooterLines) {
    List<Object> valueHolder = new ArrayList<>(skipFooterLines + 1);
    for (int i = 0; i <= skipFooterLines; i++) {
      valueHolder.add(reader.createValue());
    }
    return valueHolder;
  }
 }

<#else>
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
      while (recordCount < TARGET_RECORD_COUNT && reader.next(key, value)) {
        Object deSerializedValue = partitionSerDe.deserialize((Writable) value);
        if (partTblObjectInspectorConverter != null) {
          deSerializedValue = partTblObjectInspectorConverter.convert(deSerializedValue);
        }
        readHiveRecordAndInsertIntoRecordBatch(deSerializedValue, recordCount);
        recordCount++;
      }

      setValueCountAndPopulatePartitionVectors(recordCount);
      return recordCount;
    } catch (IOException | SerDeException e) {
      throw new DrillRuntimeException(e);
    }
  }
</#if>

}
</#list>