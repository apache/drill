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
package org.apache.drill.exec.store.parquet;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.store.TimedRunnable;
import org.apache.drill.exec.store.dfs.DrillPathFilter;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Metadata {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Metadata.class);

  public static final String[] OLD_METADATA_FILENAMES = {".drill.parquet_metadata"};
  public static final String METADATA_FILENAME = ".drill.parquet_metadata.v2";

  private final FileSystem fs;

  /**
   * Create the parquet metadata file for the directory at the given path, and for any subdirectories
   * @param fs
   * @param path
   * @throws IOException
   */
  public static void createMeta(FileSystem fs, String path) throws IOException {
    Metadata metadata = new Metadata(fs);
    metadata.createMetaFilesRecursively(path);
  }

  /**
   * Get the parquet metadata for the parquet files in the given directory, including those in subdirectories
   * @param fs
   * @param path
   * @return
   * @throws IOException
   */
  public static ParquetTableMetadata_v1 getParquetTableMetadata(FileSystem fs, String path) throws IOException {
    Metadata metadata = new Metadata(fs);
    return metadata.getParquetTableMetadata(path);
  }

  /**
   * Get the parquet metadata for a list of parquet files
   * @param fs
   * @param fileStatuses
   * @return
   * @throws IOException
   */
  public static ParquetTableMetadata_v1 getParquetTableMetadata(FileSystem fs,
                                                             List<FileStatus> fileStatuses) throws IOException {
    Metadata metadata = new Metadata(fs);
    return metadata.getParquetTableMetadata(fileStatuses);
  }

  /**
   * Get the parquet metadata for a directory by reading the metadata file
   * @param fs
   * @param path The path to the metadata file, located in the directory that contains the parquet files
   * @return
   * @throws IOException
   */
  public static ParquetTableMetadata_v1 readBlockMeta(FileSystem fs, String path) throws IOException {
    Metadata metadata = new Metadata(fs);
    return metadata.readBlockMeta(path);
  }

  private Metadata(FileSystem fs) {
    this.fs = fs;
  }

  /**
   * Create the parquet metadata file for the directory at the given path, and for any subdirectories
   * @param path
   * @throws IOException
   */
  private ParquetTableMetadata_v1 createMetaFilesRecursively(final String path) throws IOException {
    List<ParquetFileMetadata> metaDataList = Lists.newArrayList();
    List<String> directoryList = Lists.newArrayList();
    ConcurrentHashMap<ColumnTypeMetadata.Key, ColumnTypeMetadata> columnTypeInfoSet = new ConcurrentHashMap<>();
    Path p = new Path(path);
    FileStatus fileStatus = fs.getFileStatus(p);
    assert fileStatus.isDirectory() : "Expected directory";

    final List<FileStatus> childFiles = Lists.newArrayList();

    for (final FileStatus file : fs.listStatus(p, new DrillPathFilter())) {
      if (file.isDirectory()) {
        ParquetTableMetadata_v1 subTableMetadata = createMetaFilesRecursively(file.getPath().toString());
        metaDataList.addAll(subTableMetadata.files);
        directoryList.addAll(subTableMetadata.directories);
        directoryList.add(file.getPath().toString());
        // Merge the schema from the child level into the current level
        //TODO: We need a merge method that merges two colums with the same name but different types
        columnTypeInfoSet.putAll(subTableMetadata.columnTypeInfo);
      } else {
        childFiles.add(file);
      }
    }
    ParquetTableMetadata_v1 parquetTableMetadata = new ParquetTableMetadata_v1();
    if (childFiles.size() > 0) {
      List<ParquetFileMetadata> childFilesMetadata =
          getParquetFileMetadata(parquetTableMetadata, childFiles);
      metaDataList.addAll(childFilesMetadata);
      // Note that we do not need to merge the columnInfo at this point. The columnInfo is already added
      // to the parquetTableMetadata.
    }

    parquetTableMetadata.directories = directoryList;
    parquetTableMetadata.files = metaDataList;
    //TODO: We need a merge method that merges two colums with the same name but different types
    if (parquetTableMetadata.columnTypeInfo == null) {
      parquetTableMetadata.columnTypeInfo = new ConcurrentHashMap<>();
    }
    parquetTableMetadata.columnTypeInfo.putAll(columnTypeInfoSet);

    // delete old files
    for(String oldname: OLD_METADATA_FILENAMES){
      fs.delete(new Path(p, oldname), false);
    }
    writeFile(parquetTableMetadata, new Path(p, METADATA_FILENAME));
    return parquetTableMetadata;
  }

  /**
   * Get the parquet metadata for the parquet files in a directory
   * @param path the path of the directory
   * @return
   * @throws IOException
   */
  private ParquetTableMetadata_v1 getParquetTableMetadata(String path) throws IOException {
    Path p = new Path(path);
    FileStatus fileStatus = fs.getFileStatus(p);
    Stopwatch watch = new Stopwatch();
    watch.start();
    List<FileStatus> fileStatuses = getFileStatuses(fileStatus);
    logger.info("Took {} ms to get file statuses", watch.elapsed(TimeUnit.MILLISECONDS));
    watch.reset();
    watch.start();
    ParquetTableMetadata_v1 metadata_v1 = getParquetTableMetadata(fileStatuses);
    logger.info("Took {} ms to read file metadata", watch.elapsed(TimeUnit.MILLISECONDS));
    return metadata_v1;
  }

  /**
   * Get the parquet metadata for a list of parquet files
   * @param fileStatuses
   * @return
   * @throws IOException
   */
  private ParquetTableMetadata_v1 getParquetTableMetadata(List<FileStatus> fileStatuses)
      throws IOException {
    ParquetTableMetadata_v1 tableMetadata = new ParquetTableMetadata_v1();
    List<ParquetFileMetadata> fileMetadataList = getParquetFileMetadata(tableMetadata, fileStatuses);
    tableMetadata.files = fileMetadataList;
    tableMetadata.directories = new ArrayList<String>();
    return tableMetadata;
  }

  /**
   * Get a list of file metadata for a list of parquet files
   * @param fileStatuses
   * @return
   * @throws IOException
   */
  private List<ParquetFileMetadata> getParquetFileMetadata(ParquetTableMetadata_v1 parquetTableMetadata_v1,
      List<FileStatus> fileStatuses) throws IOException {
    List<TimedRunnable<ParquetFileMetadata>> gatherers = Lists.newArrayList();
    for (FileStatus file : fileStatuses) {
      gatherers.add(new MetadataGatherer(parquetTableMetadata_v1, file));
    }

    List<ParquetFileMetadata> metaDataList = Lists.newArrayList();
    metaDataList.addAll(TimedRunnable.run("Fetch parquet metadata", logger, gatherers, 16));
    return metaDataList;
  }

  /**
   * Recursively get a list of files
   * @param fileStatus
   * @return
   * @throws IOException
   */
  private List<FileStatus> getFileStatuses(FileStatus fileStatus) throws IOException {
    List<FileStatus> statuses = Lists.newArrayList();
    if (fileStatus.isDirectory()) {
      for (FileStatus child : fs.listStatus(fileStatus.getPath(), new DrillPathFilter())) {
        statuses.addAll(getFileStatuses(child));
      }
    } else {
      statuses.add(fileStatus);
    }
    return statuses;
  }

  /**
   * TimedRunnable that reads the footer from parquet and collects file metadata
   */
  private class MetadataGatherer extends TimedRunnable<ParquetFileMetadata> {

    private FileStatus fileStatus;
    private ParquetTableMetadata_v1 parquetTableMetadata;

    public MetadataGatherer(ParquetTableMetadata_v1 parquetTableMetadata, FileStatus fileStatus) {
      this.fileStatus = fileStatus;
      this.parquetTableMetadata = parquetTableMetadata;
    }

    @Override
    protected ParquetFileMetadata runInner() throws Exception {
      return getParquetFileMetadata(parquetTableMetadata, fileStatus);
    }

    @Override
    protected IOException convertToIOException(Exception e) {
      if (e instanceof IOException) {
        return (IOException) e;
      } else {
        return new IOException(e);
      }
    }
  }

  private OriginalType getOriginalType(Type type, String[] path, int depth) {
    if (type.isPrimitive()) {
      return type.getOriginalType();
    }
    Type t = ((GroupType) type).getType(path[depth]);
    return getOriginalType(t, path, depth + 1);
  }

  /**
   * Get the metadata for a single file
   * @param file
   * @return
   * @throws IOException
   */
  private ParquetFileMetadata getParquetFileMetadata(ParquetTableMetadata_v1 parquetTableMetadata,
      FileStatus file) throws IOException {
    ParquetMetadata metadata = ParquetFileReader.readFooter(fs.getConf(), file);
    MessageType schema = metadata.getFileMetaData().getSchema();

    Map<SchemaPath,OriginalType> originalTypeMap = Maps.newHashMap();
    schema.getPaths();
    for (String[] path : schema.getPaths()) {
      originalTypeMap.put(SchemaPath.getCompoundPath(path), getOriginalType(schema, path, 0));
    }

    List<RowGroupMetadata> rowGroupMetadataList = Lists.newArrayList();

    for (BlockMetaData rowGroup : metadata.getBlocks()) {
      List<ColumnMetadata> columnMetadataList = Lists.newArrayList();
      long length = 0;
      for (ColumnChunkMetaData col : rowGroup.getColumns()) {
        ColumnMetadata columnMetadata;

        boolean statsAvailable = (col.getStatistics() != null && !col.getStatistics().isEmpty());

        Statistics stats = col.getStatistics();
        String[] columnName = col.getPath().toArray() ;
        SchemaPath columnSchemaName = SchemaPath.getCompoundPath(columnName);
        ColumnTypeMetadata columnTypeMetadata =
            new ColumnTypeMetadata(columnName, col.getType(), originalTypeMap.get(columnSchemaName));
        if (parquetTableMetadata.columnTypeInfo == null) {
          parquetTableMetadata.columnTypeInfo = new ConcurrentHashMap<>();
        }
        // Save the column schema info. We'll merge it into one list
        parquetTableMetadata.columnTypeInfo
            .put(new ColumnTypeMetadata.Key(columnTypeMetadata.name), columnTypeMetadata);
        if (statsAvailable) {
          // Write stats only if minVal==maxVal. Also, we then store only maxVal
          Object mxValue = null;
          if (stats.genericGetMax() != null && stats.genericGetMin() != null && stats.genericGetMax()
              .equals(stats.genericGetMin())) {
            mxValue = stats.genericGetMax();
          }
          columnMetadata =
              new ColumnMetadata(columnTypeMetadata.name, col.getType(), mxValue, stats.getNumNulls());
        } else {
          columnMetadata = new ColumnMetadata(columnTypeMetadata.name, col.getType(), null, null);
        }
        columnMetadataList.add(columnMetadata);
        length += col.getTotalSize();
      }

      RowGroupMetadata rowGroupMeta = new RowGroupMetadata(rowGroup.getStartingPos(), length, rowGroup.getRowCount(),
              getHostAffinity(file, rowGroup.getStartingPos(), length), columnMetadataList);

      rowGroupMetadataList.add(rowGroupMeta);
    }
    String path = Path.getPathWithoutSchemeAndAuthority(file.getPath()).toString();

    return new ParquetFileMetadata(path, file.getLen(), rowGroupMetadataList);
  }

  /**
   * Get the host affinity for a row group
   * @param fileStatus the parquet file
   * @param start the start of the row group
   * @param length the length of the row group
   * @return
   * @throws IOException
   */
  private Map<String,Float> getHostAffinity(FileStatus fileStatus, long start, long length) throws IOException {
    BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, start, length);
    Map<String,Float> hostAffinityMap = Maps.newHashMap();
    for (BlockLocation blockLocation : blockLocations) {
      for (String host : blockLocation.getHosts()) {
        Float currentAffinity = hostAffinityMap.get(host);
        float blockStart = blockLocation.getOffset();
        float blockEnd = blockStart + blockLocation.getLength();
        float rowGroupEnd = start + length;
        Float newAffinity = (blockLocation.getLength() - (blockStart < start ? start - blockStart : 0) -
                (blockEnd > rowGroupEnd ? blockEnd - rowGroupEnd : 0)) / length;
        if (currentAffinity != null) {
          hostAffinityMap.put(host, currentAffinity + newAffinity);
        } else {
          hostAffinityMap.put(host, newAffinity);
        }
      }
    }
    return hostAffinityMap;
  }

  /**
   * Serialize parquet metadata to json and write to a file
   * @param parquetTableMetadata
   * @param p
   * @throws IOException
   */
  private void writeFile(ParquetTableMetadata_v1 parquetTableMetadata, Path p) throws IOException {
    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.configure(Feature.AUTO_CLOSE_TARGET, false);
    jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    ObjectMapper mapper = new ObjectMapper(jsonFactory);
    SimpleModule module = new SimpleModule();
    module.addSerializer(ColumnMetadata.class, new ColumnMetadata.Serializer());
    mapper.registerModule(module);
    FSDataOutputStream os = fs.create(p);
    mapper.writerWithDefaultPrettyPrinter().writeValue(os, parquetTableMetadata);
    os.flush();
    os.close();
  }

  /**
   * Read the parquet metadata from a file
   * @param path
   * @return
   * @throws IOException
   */
  private ParquetTableMetadata_v1 readBlockMeta(String path) throws IOException {
    Stopwatch timer = new Stopwatch();
    timer.start();
    Path p = new Path(path);
    ObjectMapper mapper = new ObjectMapper();
    AfterburnerModule module = new AfterburnerModule();
    module.addKeyDeserializer(ColumnTypeMetadata.Key.class, new ColumnTypeMetadata.Key.DeSerializer());
    mapper.registerModule(module);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    FSDataInputStream is = fs.open(p);

    ParquetTableMetadata_v1 parquetTableMetadata = mapper.readValue(is, ParquetTableMetadata_v1.class);
    logger.info("Took {} ms to read metadata from cache file", timer.elapsed(TimeUnit.MILLISECONDS));
    timer.stop();
    if (tableModified(parquetTableMetadata, p)) {
      parquetTableMetadata =
          createMetaFilesRecursively(Path.getPathWithoutSchemeAndAuthority(p.getParent()).toString());
    }
    return parquetTableMetadata;
  }

  /**
   * Check if the parquet metadata needs to be updated by comparing the modification time of the directories with
   * the modification time of the metadata file
   * @param tableMetadata
   * @param metaFilePath
   * @return
   * @throws IOException
   */
  private boolean tableModified(ParquetTableMetadata_v1 tableMetadata, Path metaFilePath) throws IOException {
    long metaFileModifyTime = fs.getFileStatus(metaFilePath).getModificationTime();
    FileStatus directoryStatus = fs.getFileStatus(metaFilePath.getParent());
    if (directoryStatus.getModificationTime() > metaFileModifyTime) {
      return true;
    }
    for (String directory : tableMetadata.directories) {
      directoryStatus = fs.getFileStatus(new Path(directory));
      if (directoryStatus.getModificationTime() > metaFileModifyTime) {
        return true;
      }
    }
    return false;
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "metadata_version")
  public static class ParquetTableMetadataBase {

  }

  /**
   * Struct which contains the metadata for an entire parquet directory structure
   */
  @JsonTypeName("v2")
  public static class ParquetTableMetadata_v1 extends ParquetTableMetadataBase {
    /*
     ColumnTypeInfo is schema information from all the files and row groups, merged into
     one. To get this info, we pass the ParquetTableMetadata object all the way dow to the
     RowGroup and the column type is built there as it is read from the footer.
     */
    @JsonProperty
    public ConcurrentHashMap<ColumnTypeMetadata.Key, ColumnTypeMetadata> columnTypeInfo;
    @JsonProperty
    List<ParquetFileMetadata> files;
    @JsonProperty
    List<String> directories;

    public ParquetTableMetadata_v1() {
      super();
    }

    public ParquetTableMetadata_v1(ConcurrentHashMap<ColumnTypeMetadata.Key, ColumnTypeMetadata> columnTypeInfo,
        List<ParquetFileMetadata> files, List<String> directories) {
      this.files = files;
      this.directories = directories;
      this.columnTypeInfo = columnTypeInfo;
    }

    public ColumnTypeMetadata getColumnTypeInfo(String[] name) {
      return columnTypeInfo.get(new ColumnTypeMetadata.Key(name));
    }

  }

  /**
   * Struct which contains the metadata for a single parquet file
   */
  public static class ParquetFileMetadata {
    @JsonProperty
    public String path;
    @JsonProperty
    public Long length;
    @JsonProperty
    public List<RowGroupMetadata> rowGroups;

    public ParquetFileMetadata() {
      super();
    }

    public ParquetFileMetadata(String path, Long length, List<RowGroupMetadata> rowGroups) {
      this.path = path;
      this.length = length;
      this.rowGroups = rowGroups;
    }

    @Override
    public String toString() {
      return String.format("path: %s rowGroups: %s", path, rowGroups);
    }
  }

  /**
   * A struct that contains the metadata for a parquet row group
   */
  public static class RowGroupMetadata {
    @JsonProperty
    public Long start;
    @JsonProperty
    public Long length;
    @JsonProperty
    public Long rowCount;
    @JsonProperty
    public Map<String, Float> hostAffinity;
    @JsonProperty
    public List<ColumnMetadata> columns;

    public RowGroupMetadata() {
      super();
    }

    public RowGroupMetadata(Long start, Long length, Long rowCount,
                            Map<String, Float> hostAffinity, List<ColumnMetadata> columns) {
      this.start = start;
      this.length = length;
      this.rowCount = rowCount;
      this.hostAffinity = hostAffinity;
      this.columns = columns;
    }
  }


  public static class ColumnTypeMetadata {
    @JsonProperty public String[] name;
    @JsonProperty public PrimitiveTypeName primitiveType;
    @JsonProperty public OriginalType originalType;

    //@JsonIgnore private int hashCode = 0;

    // Key to find by name only
    @JsonIgnore private Key key;

    public ColumnTypeMetadata() {
      super();
    }

    public ColumnTypeMetadata(String[] name, PrimitiveTypeName primitiveType, OriginalType originalType) {
      this.name = name;
      this.primitiveType = primitiveType;
      this.originalType = originalType;
      this.key = new Key(name);
    }

    @JsonIgnore private Key key() {
      return this.key;
    }

    private static class Key {
      private String[] name;
      private int hashCode = 0;

      public Key(String[] name) {
        this.name = name;
      }

      @Override public int hashCode() {
        if (hashCode == 0) {
          hashCode = Arrays.hashCode(name);
        }
        return hashCode;
      }

      @Override public boolean equals(Object obj) {
        if (obj == null) {
          return false;
        }
        if (getClass() != obj.getClass()) {
          return false;
        }
        final Key other = (Key) obj;
        return Arrays.equals(this.name, other.name);
      }

      @Override public String toString() {
        String s = null;
        for (String namePart : name) {
          if (s != null) {
            s += ".";
            s += namePart;
          } else {
            s = namePart;
          }
        }
        return s;
      }

      public static class DeSerializer extends KeyDeserializer {

        public DeSerializer() {
          super();
        }

        @Override
        public Object deserializeKey(String key, com.fasterxml.jackson.databind.DeserializationContext ctxt)
            throws IOException, com.fasterxml.jackson.core.JsonProcessingException {
          return new Key(key.split("\\."));
        }
      }
    }
  }


  /**
   * A struct that contains the metadata for a column in a parquet file
   */
  public static class ColumnMetadata {
    // Use a string array for name instead of Schema Path to make serialization easier
    @JsonProperty public String[] name;
    @JsonProperty
    public Long nulls;

    public Object mxValue;

    @JsonIgnore private PrimitiveTypeName primitiveType;

    public ColumnMetadata() {
      super();
    }

    public ColumnMetadata(String[] name, PrimitiveTypeName primitiveType, Object mxValue, Long nulls) {
      this.name = name;
      this.mxValue = mxValue;
      this.nulls = nulls;
      this.primitiveType=primitiveType;
    }

    @JsonProperty(value = "mxValue")
    public void setMax(Object mxValue) {
      this.mxValue = mxValue;
    }

    public static class DeSerializer extends JsonDeserializer<ColumnMetadata> {
      @Override public ColumnMetadata deserialize(JsonParser jp, DeserializationContext ctxt)
          throws IOException, JsonProcessingException {
        return null;
      }
    }

    // WE use a custom serializer and write only non null values.
    public static class Serializer extends JsonSerializer<ColumnMetadata> {
      @Override public void serialize(ColumnMetadata value, JsonGenerator jgen, SerializerProvider provider)
          throws IOException, JsonProcessingException {
        jgen.writeStartObject();
        jgen.writeArrayFieldStart("name");
        for (String n : value.name) {
          jgen.writeString(n);
        }
        jgen.writeEndArray();
        if (value.mxValue != null) {
          Object val;
          if (value.primitiveType == PrimitiveTypeName.BINARY && value.mxValue != null) {
            val = new String(((Binary) value.mxValue).getBytes());
          } else {
            val = value.mxValue;
          }
          jgen.writeObjectField("mxValue", val);
        }
        if (value.nulls != null) {
          jgen.writeObjectField("nulls", value.nulls);
        }
        jgen.writeEndObject();
      }
    }

  }
}

