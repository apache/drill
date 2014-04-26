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

import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.commons.codec.binary.Base64;
import org.apache.drill.common.util.DataOutputOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.*;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.*;

public class HiveInputReader {
  public static void main(String args[]) throws Exception {
/*
    String[] columnNames = {"n_nationkey", "n_name", "n_regionkey",   "n_comment"};
    String[] columnTypes = {"bigint", "string", "bigint", "string"};

    List<FieldSchema> cols = Lists.newArrayList();

    for (int i = 0; i < columnNames.length; i++) {
      cols.add(new FieldSchema(columnNames[i], columnTypes[i], null));
    }
    String location = "file:///tmp/nation_s";
    String inputFormat = TextInputFormat.class.getCanonicalName();
    String serdeLib = LazySimpleSerDe.class.getCanonicalName();
//    String inputFormat = HiveHBaseTableInputFormat.class.getCanonicalName();
//    String serdeLib = HBaseSerDe.class.getCanonicalName();
    Map<String, String> serdeParams = new HashMap();
//    serdeParams.put("serialization.format", "1");
//    serdeParams.put("hbase.columns.mapping", ":key,f:name,f:regionkey,f:comment");
    serdeParams.put("serialization.format", "|");
    serdeParams.put("field.delim", "|");


    Map<String, String> tableParams = new HashMap();
    tableParams.put("hbase.table.name", "nation");
    SerDeInfo serDeInfo = new SerDeInfo(null, serdeLib, serdeParams);
    StorageDescriptor storageDescriptor = new StorageDescriptor(cols, location, inputFormat, null, false, -1, serDeInfo, null, null, null);
    Table table = new Table("table", "default", "sphillips", 0, 0, 0, storageDescriptor, new ArrayList<FieldSchema>(), tableParams, null, null, "MANAGED_TABLE");
    Properties properties = MetaStoreUtils.getTableMetadata(table);
    */

    HiveConf conf = new HiveConf();
    conf.set("hive.metastore.uris", "thrift://10.10.31.51:9083");
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    Table table = client.getTable("default", "nation");
    Properties properties = MetaStoreUtils.getTableMetadata(table);

    Path path = new Path(table.getSd().getLocation());
    JobConf job = new JobConf();
    for (Object obj : properties.keySet()) {
      job.set((String) obj, (String) properties.get(obj));
    }
//    job.set("hbase.zookeeper.quorum", "10.10.31.51");
//    job.set("hbase.zookeeper.property.clientPort", "5181");
    InputFormat f = (InputFormat) Class.forName(table.getSd().getInputFormat()).getConstructor().newInstance();
    job.setInputFormat(f.getClass());
    FileInputFormat.addInputPath(job, path);
    InputFormat format = job.getInputFormat();
    SerDe serde = (SerDe) Class.forName(table.getSd().getSerdeInfo().getSerializationLib()).getConstructor().newInstance();
    serde.initialize(job, properties);
    ObjectInspector inspector = serde.getObjectInspector();
    ObjectInspector.Category cat = inspector.getCategory();
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(inspector);
    List<String> columns = null;
    List<TypeInfo> colTypes = null;
    List<ObjectInspector> fieldObjectInspectors = Lists.newArrayList();

    switch(typeInfo.getCategory()) {
      case STRUCT:
        columns = ((StructTypeInfo) typeInfo).getAllStructFieldNames();
        colTypes = ((StructTypeInfo) typeInfo).getAllStructFieldTypeInfos();
        for (int i = 0; i < columns.size(); i++) {
          System.out.print(columns.get(i));
          System.out.print(" ");
          System.out.print(colTypes.get(i));
        }
        System.out.println("");
        for (StructField field : ((StructObjectInspector)inspector).getAllStructFieldRefs()) {
          fieldObjectInspectors.add(field.getFieldObjectInspector());
        }
    }

    for (InputSplit split : format.getSplits(job, 1)) {
      String encoded = serializeInputSplit(split);
      System.out.println(encoded);
      InputSplit newSplit = deserializeInputSplit(encoded, split.getClass().getCanonicalName());
      System.out.print("Length: " + newSplit.getLength() + " ");
      System.out.print("Locations: ");
      for (String loc : newSplit.getLocations()) System.out.print(loc + " " );
      System.out.println();
    }

    for (InputSplit split : format.getSplits(job, 1)) {
      RecordReader reader = format.getRecordReader(split, job, Reporter.NULL);
      Object key = reader.createKey();
      Object value = reader.createValue();
      int count = 0;
      while (reader.next(key, value)) {
        List<Object> values = ((StructObjectInspector) inspector).getStructFieldsDataAsList(serde.deserialize((Writable) value));
        StructObjectInspector sInsp = (StructObjectInspector) inspector;
        Object obj = sInsp.getStructFieldData(serde.deserialize((Writable) value) , sInsp.getStructFieldRef("n_name"));
        System.out.println(obj);
        /*
        for (Object obj : values) {
          PrimitiveObjectInspector.PrimitiveCategory pCat = ((PrimitiveObjectInspector)fieldObjectInspectors.get(count)).getPrimitiveCategory();
          Object pObj = ((PrimitiveObjectInspector)fieldObjectInspectors.get(count)).getPrimitiveJavaObject(obj);
          System.out.print(pObj + " ");
        }
        */
        System.out.println("");
      }
    }
  }

  public static String serializeInputSplit(InputSplit split) throws IOException {
    ByteArrayDataOutput byteArrayOutputStream =  ByteStreams.newDataOutput();
    split.write(byteArrayOutputStream);
    return Base64.encodeBase64String(byteArrayOutputStream.toByteArray());
  }

  public static InputSplit deserializeInputSplit(String base64, String className) throws Exception {
    InputSplit split;
    if (Class.forName(className) == FileSplit.class) {
      split = new FileSplit((Path) null, 0, 0, (String[])null);
    } else {
      split = (InputSplit) Class.forName(className).getConstructor().newInstance();
    }
    ByteArrayDataInput byteArrayDataInput = ByteStreams.newDataInput(Base64.decodeBase64(base64));
    split.readFields(byteArrayDataInput);
    return split;
  }
}

