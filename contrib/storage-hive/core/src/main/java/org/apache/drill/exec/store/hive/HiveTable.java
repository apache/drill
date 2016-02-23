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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;

@JsonTypeName("table")
public class HiveTable {

  @JsonIgnore
  private Table table;

  @JsonProperty
  public String tableName;
  @JsonProperty
  public String dbName;
  @JsonProperty
  public String owner;
  @JsonProperty
  public int createTime;
  @JsonProperty
  public int lastAccessTime;
  @JsonProperty
  public int retention;
  @JsonProperty
  public StorageDescriptorWrapper sd;
  @JsonProperty
  public List<FieldSchemaWrapper> partitionKeys;
  @JsonProperty
  public Map<String,String> parameters;
  @JsonProperty
  public String viewOriginalText;
  @JsonProperty
  public String viewExpandedText;
  @JsonProperty
  public String tableType;

  @JsonIgnore
  public final Map<String, String> partitionNameTypeMap = new HashMap<>();

  @JsonCreator
  public HiveTable(@JsonProperty("tableName") String tableName, @JsonProperty("dbName") String dbName, @JsonProperty("owner") String owner, @JsonProperty("createTime") int createTime,
                   @JsonProperty("lastAccessTime") int lastAccessTime, @JsonProperty("retention") int retention, @JsonProperty("sd") StorageDescriptorWrapper sd,
                   @JsonProperty("partitionKeys") List<FieldSchemaWrapper> partitionKeys, @JsonProperty("parameters") Map<String, String> parameters,
                   @JsonProperty("viewOriginalText") String viewOriginalText, @JsonProperty("viewExpandedText") String viewExpandedText, @JsonProperty("tableType") String tableType
  ) {
    this.tableName = tableName;
    this.dbName = dbName;
    this.owner = owner;
    this.createTime = createTime;
    this.lastAccessTime = lastAccessTime;
    this.retention = retention;
    this.sd = sd;
    this.partitionKeys = partitionKeys;
    this.parameters = parameters;
    this.viewOriginalText = viewOriginalText;
    this.viewExpandedText = viewExpandedText;
    this.tableType = tableType;

    List<FieldSchema> partitionKeysUnwrapped = Lists.newArrayList();
    for (FieldSchemaWrapper w : partitionKeys) {
      partitionKeysUnwrapped.add(w.getFieldSchema());
      partitionNameTypeMap.put(w.name, w.type);
    }
    StorageDescriptor sdUnwrapped = sd.getSd();
    this.table = new Table(tableName, dbName, owner, createTime, lastAccessTime, retention, sdUnwrapped, partitionKeysUnwrapped,
        parameters, viewOriginalText, viewExpandedText, tableType);
  }

  public HiveTable(Table table) {
    if (table == null) {
      return;
    }
    this.table = table;
    this.tableName = table.getTableName();
    this.dbName = table.getDbName();
    this.owner = table.getOwner();
    this.createTime = table.getCreateTime();
    this.lastAccessTime = table.getLastAccessTime();
    this.retention = table.getRetention();
    this.sd = new StorageDescriptorWrapper(table.getSd());
    this.partitionKeys = Lists.newArrayList();
    for (FieldSchema f : table.getPartitionKeys()) {
      this.partitionKeys.add(new FieldSchemaWrapper(f));
      partitionNameTypeMap.put(f.getName(), f.getType());
    }
    this.parameters = table.getParameters();
    this.viewOriginalText = table.getViewOriginalText();
    this.viewExpandedText = table.getViewExpandedText();
    this.tableType = table.getTableType();
  }

  @JsonIgnore
  public Table getTable() {
    return table;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Table(");

    sb.append("dbName:");
    sb.append(this.dbName);
    sb.append(", ");

    sb.append("tableName:");
    sb.append(this.tableName);
    sb.append(")");

    return sb.toString();
  }

  public static class HivePartition {

    @JsonIgnore
    private Partition partition;

    @JsonProperty
    public List<String> values;
    @JsonProperty
    public String tableName;
    @JsonProperty
    public String dbName;
    @JsonProperty
    public int createTime;
    @JsonProperty
    public int lastAccessTime;
    @JsonProperty
    public StorageDescriptorWrapper sd;
    @JsonProperty
    public Map<String,String> parameters;

    @JsonCreator
    public HivePartition(@JsonProperty("values") List<String> values, @JsonProperty("tableName") String tableName, @JsonProperty("dbName") String dbName, @JsonProperty("createTime") int createTime,
                         @JsonProperty("lastAccessTime") int lastAccessTime,  @JsonProperty("sd") StorageDescriptorWrapper sd,
                         @JsonProperty("parameters") Map<String, String> parameters
    ) {
      this.values = values;
      this.tableName = tableName;
      this.dbName = dbName;
      this.createTime = createTime;
      this.lastAccessTime = lastAccessTime;
      this.sd = sd;
      this.parameters = parameters;

      StorageDescriptor sdUnwrapped = sd.getSd();
      this.partition = new org.apache.hadoop.hive.metastore.api.Partition(values, tableName, dbName, createTime, lastAccessTime, sdUnwrapped, parameters);
    }

    public HivePartition(Partition partition) {
      if (partition == null) {
        return;
      }
      this.partition = partition;
      this.values = partition.getValues();
      this.tableName = partition.getTableName();
      this.dbName = partition.getDbName();
      this.createTime = partition.getCreateTime();
      this.lastAccessTime = partition.getLastAccessTime();
      this.sd = new StorageDescriptorWrapper(partition.getSd());
      this.parameters = partition.getParameters();
    }

    @JsonIgnore
    public Partition getPartition() {
      return partition;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("Partition(");
      sb.append("values:");
      sb.append(this.values);
      sb.append(")");
      return sb.toString();
    }
  }

  public static class StorageDescriptorWrapper {
    @JsonIgnore
    private StorageDescriptor sd;
    @JsonProperty
    public List<FieldSchemaWrapper> cols;
    @JsonProperty
    public String location;
    @JsonProperty
    public String inputFormat;
    @JsonProperty
    public String outputFormat;
    @JsonProperty
    public boolean compressed;
    @JsonProperty
    public int numBuckets;
    @JsonProperty
    public SerDeInfoWrapper serDeInfo;
    //    @JsonProperty
//    public List<String> bucketCols;
    @JsonProperty
    public List<OrderWrapper> sortCols;
    @JsonProperty
    public Map<String,String> parameters;

    @JsonCreator
    public StorageDescriptorWrapper(@JsonProperty("cols") List<FieldSchemaWrapper> cols, @JsonProperty("location") String location, @JsonProperty("inputFormat") String inputFormat,
                                    @JsonProperty("outputFormat") String outputFormat, @JsonProperty("compressed") boolean compressed, @JsonProperty("numBuckets") int numBuckets,
                                    @JsonProperty("serDeInfo") SerDeInfoWrapper serDeInfo,  @JsonProperty("sortCols") List<OrderWrapper> sortCols,
                                    @JsonProperty("parameters") Map<String,String> parameters) {
      this.cols = cols;
      this.location = location;
      this.inputFormat = inputFormat;
      this.outputFormat = outputFormat;
      this.compressed = compressed;
      this.numBuckets = numBuckets;
      this.serDeInfo = serDeInfo;
//      this.bucketCols = bucketCols;
      this.sortCols = sortCols;
      this.parameters = parameters;
      List<FieldSchema> colsUnwrapped = Lists.newArrayList();
      for (FieldSchemaWrapper w: cols) {
        colsUnwrapped.add(w.getFieldSchema());
      }
      SerDeInfo serDeInfoUnwrapped = serDeInfo.getSerDeInfo();
      List<Order> sortColsUnwrapped = Lists.newArrayList();
      for (OrderWrapper w : sortCols) {
        sortColsUnwrapped.add(w.getOrder());
      }
//      this.sd = new StorageDescriptor(colsUnwrapped, location, inputFormat, outputFormat, compressed, numBuckets, serDeInfoUnwrapped,
//              bucketCols, sortColsUnwrapped, parameters);
      this.sd = new StorageDescriptor(colsUnwrapped, location, inputFormat, outputFormat, compressed, numBuckets, serDeInfoUnwrapped,
          null, sortColsUnwrapped, parameters);
    }

    public StorageDescriptorWrapper(StorageDescriptor sd) {
      this.sd = sd;
      this.cols = Lists.newArrayList();
      for (FieldSchema f : sd.getCols()) {
        this.cols.add(new FieldSchemaWrapper(f));
      }
      this.location = sd.getLocation();
      this.inputFormat = sd.getInputFormat();
      this.outputFormat = sd.getOutputFormat();
      this.compressed = sd.isCompressed();
      this.numBuckets = sd.getNumBuckets();
      this.serDeInfo = new SerDeInfoWrapper(sd.getSerdeInfo());
//      this.bucketCols = sd.getBucketCols();
      this.sortCols = Lists.newArrayList();
      for (Order o : sd.getSortCols()) {
        this.sortCols.add(new OrderWrapper(o));
      }
      this.parameters = sd.getParameters();
    }

    @JsonIgnore
    public StorageDescriptor getSd() {
      return sd;
    }

  }

  public static class SerDeInfoWrapper {
    @JsonIgnore
    private SerDeInfo serDeInfo;
    @JsonProperty
    public String name;
    @JsonProperty
    public String serializationLib;
    @JsonProperty
    public Map<String,String> parameters;

    @JsonCreator
    public SerDeInfoWrapper(@JsonProperty("name") String name, @JsonProperty("serializationLib") String serializationLib, @JsonProperty("parameters") Map<String, String> parameters) {
      this.name = name;
      this.serializationLib = serializationLib;
      this.parameters = parameters;
      this.serDeInfo = new SerDeInfo(name, serializationLib, parameters);
    }

    public SerDeInfoWrapper(SerDeInfo serDeInfo) {
      this.serDeInfo = serDeInfo;
      this.name = serDeInfo.getName();
      this.serializationLib = serDeInfo.getSerializationLib();
      this.parameters = serDeInfo.getParameters();
    }

    @JsonIgnore
    public SerDeInfo getSerDeInfo() {
      return serDeInfo;
    }
  }

  public static class FieldSchemaWrapper {
    @JsonIgnore
    private FieldSchema fieldSchema;
    @JsonProperty
    public String name;
    @JsonProperty
    public String type;
    @JsonProperty
    public String comment;

    @JsonCreator
    public FieldSchemaWrapper(@JsonProperty("name") String name, @JsonProperty("type") String type, @JsonProperty("comment") String comment) {
      this.name = name;
      this.type = type;
      this.comment = comment;
      this.fieldSchema = new FieldSchema(name, type, comment);
    }

    public FieldSchemaWrapper(FieldSchema fieldSchema) {
      this.fieldSchema = fieldSchema;
      this.name = fieldSchema.getName();
      this.type = fieldSchema.getType();
      this.comment = fieldSchema.getComment();
    }

    @JsonIgnore
    public FieldSchema getFieldSchema() {
      return fieldSchema;
    }
  }

  public static class OrderWrapper {
    @JsonIgnore
    private Order ord;
    @JsonProperty
    public String col;
    @JsonProperty
    public int order;

    @JsonCreator
    public OrderWrapper(@JsonProperty("col") String col, @JsonProperty("order") int order) {
      this.col = col;
      this.order = order;
    }

    public OrderWrapper(Order ord) {
      this.ord = ord;
      this.col = ord.getCol();
      this.order = ord.getOrder();
    }

    @JsonIgnore
    public Order getOrder() {
      return ord;
    }
  }

  public Map<String, String> getPartitionNameTypeMap() {
    return partitionNameTypeMap;
  }

}
