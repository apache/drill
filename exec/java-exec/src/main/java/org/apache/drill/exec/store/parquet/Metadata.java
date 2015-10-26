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
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.SchemaPath.De;
import org.apache.drill.exec.store.TimedRunnable;
import org.apache.drill.exec.store.dfs.DrillPathFilter;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import parquet.column.statistics.Statistics;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.io.api.Binary;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Metadata {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Metadata.class);

  public static final String METADATA_FILENAME = ".drill.parquet_metadata";

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
      } else {
        childFiles.add(file);
      }
    }
    if (childFiles.size() > 0) {
      metaDataList.addAll(getParquetFileMetadata(childFiles));
    }
    ParquetTableMetadata_v1 parquetTableMetadata = new ParquetTableMetadata_v1(metaDataList, directoryList);
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
    return getParquetTableMetadata(fileStatuses);
  }

  /**
   * Get the parquet metadata for a list of parquet files
   * @param fileStatuses
   * @return
   * @throws IOException
   */
  private ParquetTableMetadata_v1 getParquetTableMetadata(List<FileStatus> fileStatuses) throws IOException {
    List<ParquetFileMetadata> fileMetadataList = getParquetFileMetadata(fileStatuses);
    return new ParquetTableMetadata_v1(fileMetadataList, new ArrayList<String>());
  }

  /**
   * Get a list of file metadata for a list of parquet files
   * @param fileStatuses
   * @return
   * @throws IOException
   */
  private List<ParquetFileMetadata> getParquetFileMetadata(List<FileStatus> fileStatuses) throws IOException {
    List<TimedRunnable<ParquetFileMetadata>> gatherers = Lists.newArrayList();
    for (FileStatus file : fileStatuses) {
      gatherers.add(new MetadataGatherer(file));
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

    public MetadataGatherer(FileStatus fileStatus) {
      this.fileStatus = fileStatus;
    }

    @Override
    protected ParquetFileMetadata runInner() throws Exception {
      return getParquetFileMetadata(fileStatus);
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
  private ParquetFileMetadata getParquetFileMetadata(FileStatus file) throws IOException {
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
        SchemaPath columnName = SchemaPath.getCompoundPath(col.getPath().toArray());
        if (statsAvailable) {
          columnMetadata = new ColumnMetadata(columnName, col.getType(), originalTypeMap.get(columnName),
              stats.genericGetMax(), stats.genericGetMin(), stats.getNumNulls());
        } else {
          columnMetadata = new ColumnMetadata(columnName, col.getType(), originalTypeMap.get(columnName),
              null, null, null);
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
    Path p = new Path(path);
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addDeserializer(SchemaPath.class, new De());
    mapper.registerModule(module);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    FSDataInputStream is = fs.open(p);
    ParquetTableMetadata_v1 parquetTableMetadata = mapper.readValue(is, ParquetTableMetadata_v1.class);
    if (tableModified(parquetTableMetadata, p)) {
      parquetTableMetadata = createMetaFilesRecursively(Path.getPathWithoutSchemeAndAuthority(p.getParent()).toString());
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
  @JsonTypeName("v1")
  public static class ParquetTableMetadata_v1 extends ParquetTableMetadataBase {
    @JsonProperty
    List<ParquetFileMetadata> files;
    @JsonProperty
    List<String> directories;

    public ParquetTableMetadata_v1() {
      super();
    }

    public ParquetTableMetadata_v1(List<ParquetFileMetadata> files, List<String> directories) {
      this.files = files;
      this.directories = directories;
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

  /**
   * A struct that contains the metadata for a column in a parquet file
   */
  public static class ColumnMetadata {
    @JsonProperty
    public SchemaPath name;
    @JsonProperty
    public PrimitiveTypeName primitiveType;
    @JsonProperty
    public OriginalType originalType;
    @JsonProperty
    public Long nulls;

    // JsonProperty for these are associated with the getters and setters
    public Object max;
    public Object min;


    public ColumnMetadata() {
      super();
    }

    public ColumnMetadata(SchemaPath name, PrimitiveTypeName primitiveType, OriginalType originalType,
                          Object max, Object min, Long nulls) {
      this.name = name;
      this.primitiveType = primitiveType;
      this.originalType = originalType;
      this.max = max;
      this.min = min;
      this.nulls = nulls;
    }

    @JsonProperty(value = "min")
    public Object getMin() {
      if (primitiveType == PrimitiveTypeName.BINARY && min != null) {
         return new String(((Binary) min).getBytes());
      }
      return min;
    }

    @JsonProperty(value = "max")
    public Object getMax() {
      if (primitiveType == PrimitiveTypeName.BINARY && max != null) {
        return new String(((Binary) max).getBytes());
      }
      return max;
    }

    /**
     * setter used during deserialization of the 'min' field of the metadata cache file.
     * @param min
     */
    @JsonProperty(value = "min")
    public void setMin(Object min) {
      this.min = min;
     }

    /**
     * setter used during deserialization of the 'max' field of the metadata cache file.
     * @param max
     */
    @JsonProperty(value = "max")
    public void setMax(Object max) {
      this.max = max;
    }

  }
}
