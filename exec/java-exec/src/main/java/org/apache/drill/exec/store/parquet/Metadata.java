/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Iterator;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.DrillVersionInfo;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.TimedRunnable;
import org.apache.drill.exec.store.dfs.DrillPathFilter;
import org.apache.drill.exec.store.dfs.MetadataContext;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class Metadata {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Metadata.class);

  public static final String[] OLD_METADATA_FILENAMES = {".drill.parquet_metadata.v2"};
  public static final String METADATA_FILENAME = ".drill.parquet_metadata";
  public static final String METADATA_DIRECTORIES_FILENAME = ".drill.parquet_metadata_directories";

  private final FileSystem fs;
  private final ParquetFormatConfig formatConfig;

  private ParquetTableMetadataBase parquetTableMetadata;
  private ParquetTableMetadataDirs parquetTableMetadataDirs;

  /**
   * Create the parquet metadata file for the directory at the given path, and for any subdirectories
   *
   * @param fs
   * @param path
   * @throws IOException
   */
  public static void createMeta(FileSystem fs, String path, ParquetFormatConfig formatConfig) throws IOException {
    Metadata metadata = new Metadata(fs, formatConfig);
    metadata.createMetaFilesRecursively(path);
  }

  /**
   * Get the parquet metadata for the parquet files in the given directory, including those in subdirectories
   *
   * @param fs
   * @param path
   * @return
   * @throws IOException
   */
  public static ParquetTableMetadata_v3 getParquetTableMetadata(FileSystem fs, String path, ParquetFormatConfig formatConfig)
      throws IOException {
    Metadata metadata = new Metadata(fs, formatConfig);
    return metadata.getParquetTableMetadata(path);
  }

  /**
   * Get the parquet metadata for a list of parquet files
   *
   * @param fs
   * @param fileStatuses
   * @return
   * @throws IOException
   */
  public static ParquetTableMetadata_v3 getParquetTableMetadata(FileSystem fs,
      List<FileStatus> fileStatuses, ParquetFormatConfig formatConfig) throws IOException {
    Metadata metadata = new Metadata(fs, formatConfig);
    return metadata.getParquetTableMetadata(fileStatuses);
  }

  /**
   * Get the parquet metadata for a directory by reading the metadata file
   *
   * @param fs
   * @param path The path to the metadata file, located in the directory that contains the parquet files
   * @return
   * @throws IOException
   */
  public static ParquetTableMetadataBase readBlockMeta(FileSystem fs, String path, MetadataContext metaContext, ParquetFormatConfig formatConfig) throws IOException {
    Metadata metadata = new Metadata(fs, formatConfig);
    metadata.readBlockMeta(path, false, metaContext);
    return metadata.parquetTableMetadata;
  }

  public static ParquetTableMetadataDirs readMetadataDirs(FileSystem fs, String path, MetadataContext metaContext, ParquetFormatConfig formatConfig) throws IOException {
    Metadata metadata = new Metadata(fs, formatConfig);
    metadata.readBlockMeta(path, true, metaContext);
    return metadata.parquetTableMetadataDirs;
  }

  private Metadata(FileSystem fs, ParquetFormatConfig formatConfig) {
    this.fs = ImpersonationUtil.createFileSystem(ImpersonationUtil.getProcessUserName(), fs.getConf());
    this.formatConfig = formatConfig;
  }

  /**
   * Create the parquet metadata file for the directory at the given path, and for any subdirectories
   *
   * @param path
   * @throws IOException
   */
  private Pair<ParquetTableMetadata_v3, ParquetTableMetadataDirs>
  createMetaFilesRecursively(final String path) throws IOException {
    Stopwatch timer = Stopwatch.createStarted();
    List<ParquetFileMetadata_v3> metaDataList = Lists.newArrayList();
    List<String> directoryList = Lists.newArrayList();
    ConcurrentHashMap<ColumnTypeMetadata_v3.Key, ColumnTypeMetadata_v3> columnTypeInfoSet =
        new ConcurrentHashMap<>();
    Path p = new Path(path);
    FileStatus fileStatus = fs.getFileStatus(p);
    assert fileStatus.isDirectory() : "Expected directory";

    final List<FileStatus> childFiles = Lists.newArrayList();

    for (final FileStatus file : fs.listStatus(p, new DrillPathFilter())) {
      if (file.isDirectory()) {
        ParquetTableMetadata_v3 subTableMetadata = (createMetaFilesRecursively(file.getPath().toString())).getLeft();
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
    ParquetTableMetadata_v3 parquetTableMetadata = new ParquetTableMetadata_v3(DrillVersionInfo.getVersion());
    if (childFiles.size() > 0) {
      List<ParquetFileMetadata_v3 > childFilesMetadata =
          getParquetFileMetadata_v3(parquetTableMetadata, childFiles);
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

    for (String oldname : OLD_METADATA_FILENAMES) {
      fs.delete(new Path(p, oldname), false);
    }
    writeFile(parquetTableMetadata, new Path(p, METADATA_FILENAME));

    if (directoryList.size() > 0 && childFiles.size() == 0) {
      ParquetTableMetadataDirs parquetTableMetadataDirs = new ParquetTableMetadataDirs(directoryList);
      writeFile(parquetTableMetadataDirs, new Path(p, METADATA_DIRECTORIES_FILENAME));
      logger.info("Creating metadata files recursively took {} ms", timer.elapsed(TimeUnit.MILLISECONDS));
      timer.stop();
      return Pair.of(parquetTableMetadata, parquetTableMetadataDirs);
    }
    List<String> emptyDirList = Lists.newArrayList();
    logger.info("Creating metadata files recursively took {} ms", timer.elapsed(TimeUnit.MILLISECONDS));
    timer.stop();
    return Pair.of(parquetTableMetadata, new ParquetTableMetadataDirs(emptyDirList));
  }

  /**
   * Get the parquet metadata for the parquet files in a directory
   *
   * @param path the path of the directory
   * @return
   * @throws IOException
   */
  private ParquetTableMetadata_v3 getParquetTableMetadata(String path) throws IOException {
    Path p = new Path(path);
    FileStatus fileStatus = fs.getFileStatus(p);
    final Stopwatch watch = Stopwatch.createStarted();
    List<FileStatus> fileStatuses = getFileStatuses(fileStatus);
    logger.info("Took {} ms to get file statuses", watch.elapsed(TimeUnit.MILLISECONDS));
    watch.reset();
    watch.start();
    ParquetTableMetadata_v3 metadata_v3 = getParquetTableMetadata(fileStatuses);
    logger.info("Took {} ms to read file metadata", watch.elapsed(TimeUnit.MILLISECONDS));
    return metadata_v3;
  }

  /**
   * Get the parquet metadata for a list of parquet files
   *
   * @param fileStatuses
   * @return
   * @throws IOException
   */
  private ParquetTableMetadata_v3 getParquetTableMetadata(List<FileStatus> fileStatuses)
      throws IOException {
    ParquetTableMetadata_v3 tableMetadata = new ParquetTableMetadata_v3();
    List<ParquetFileMetadata_v3> fileMetadataList = getParquetFileMetadata_v3(tableMetadata, fileStatuses);
    tableMetadata.files = fileMetadataList;
    tableMetadata.directories = new ArrayList<String>();
    return tableMetadata;
  }

  /**
   * Get a list of file metadata for a list of parquet files
   *
   * @param fileStatuses
   * @return
   * @throws IOException
   */
  private List<ParquetFileMetadata_v3> getParquetFileMetadata_v3(
      ParquetTableMetadata_v3 parquetTableMetadata_v3, List<FileStatus> fileStatuses) throws IOException {
    List<TimedRunnable<ParquetFileMetadata_v3>> gatherers = Lists.newArrayList();
    for (FileStatus file : fileStatuses) {
      gatherers.add(new MetadataGatherer(parquetTableMetadata_v3, file));
    }

    List<ParquetFileMetadata_v3> metaDataList = Lists.newArrayList();
    metaDataList.addAll(TimedRunnable.run("Fetch parquet metadata", logger, gatherers, 16));
    return metaDataList;
  }

  /**
   * Recursively get a list of files
   *
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
  private class MetadataGatherer extends TimedRunnable<ParquetFileMetadata_v3> {

    private FileStatus fileStatus;
    private ParquetTableMetadata_v3 parquetTableMetadata;

    public MetadataGatherer(ParquetTableMetadata_v3 parquetTableMetadata, FileStatus fileStatus) {
      this.fileStatus = fileStatus;
      this.parquetTableMetadata = parquetTableMetadata;
    }

    @Override
    protected ParquetFileMetadata_v3 runInner() throws Exception {
      return getParquetFileMetadata_v3(parquetTableMetadata, fileStatus);
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

  private ColTypeInfo getColTypeInfo(MessageType schema, Type type, String[] path, int depth) {
    if (type.isPrimitive()) {
      PrimitiveType primitiveType = (PrimitiveType) type;
      int precision = 0;
      int scale = 0;
      if (primitiveType.getDecimalMetadata() != null) {
        precision = primitiveType.getDecimalMetadata().getPrecision();
        scale = primitiveType.getDecimalMetadata().getScale();
      }

      int repetitionLevel = schema.getMaxRepetitionLevel(path);
      int definitionLevel = schema.getMaxDefinitionLevel(path);

      return new ColTypeInfo(type.getOriginalType(), precision, scale, repetitionLevel, definitionLevel);
    }
    Type t = ((GroupType) type).getType(path[depth]);
    return getColTypeInfo(schema, t, path, depth + 1);
  }

  private class ColTypeInfo {
    public OriginalType originalType;
    public int precision;
    public int scale;
    public int repetitionLevel;
    public int definitionLevel;

    public ColTypeInfo(OriginalType originalType, int precision, int scale, int repetitionLevel, int definitionLevel) {
      this.originalType = originalType;
      this.precision = precision;
      this.scale = scale;
      this.repetitionLevel = repetitionLevel;
      this.definitionLevel = definitionLevel;
    }
  }

  /**
   * Get the metadata for a single file
   *
   * @param file
   * @return
   * @throws IOException
   */
  private ParquetFileMetadata_v3 getParquetFileMetadata_v3(ParquetTableMetadata_v3 parquetTableMetadata,
      FileStatus file) throws IOException {
    ParquetMetadata metadata = ParquetFileReader.readFooter(fs.getConf(), file);
    MessageType schema = metadata.getFileMetaData().getSchema();

//    Map<SchemaPath, OriginalType> originalTypeMap = Maps.newHashMap();
    Map<SchemaPath, ColTypeInfo> colTypeInfoMap = Maps.newHashMap();
    schema.getPaths();
    for (String[] path : schema.getPaths()) {
      colTypeInfoMap.put(SchemaPath.getCompoundPath(path), getColTypeInfo(schema, schema, path, 0));
    }

    List<RowGroupMetadata_v3> rowGroupMetadataList = Lists.newArrayList();

    ArrayList<SchemaPath> ALL_COLS = new ArrayList<>();
    ALL_COLS.add(AbstractRecordReader.STAR_COLUMN);
    boolean autoCorrectCorruptDates = formatConfig.autoCorrectCorruptDates;
    ParquetReaderUtility.DateCorruptionStatus containsCorruptDates = ParquetReaderUtility.detectCorruptDates(metadata, ALL_COLS, autoCorrectCorruptDates);
    if (logger.isDebugEnabled()) {
      logger.debug(containsCorruptDates.toString());
    }
    for (BlockMetaData rowGroup : metadata.getBlocks()) {
      List<ColumnMetadata_v3> columnMetadataList = Lists.newArrayList();
      long length = 0;
      for (ColumnChunkMetaData col : rowGroup.getColumns()) {
        ColumnMetadata_v3 columnMetadata;

        boolean statsAvailable = (col.getStatistics() != null && !col.getStatistics().isEmpty());

        Statistics<?> stats = col.getStatistics();
        String[] columnName = col.getPath().toArray();
        SchemaPath columnSchemaName = SchemaPath.getCompoundPath(columnName);
        ColTypeInfo colTypeInfo = colTypeInfoMap.get(columnSchemaName);

        ColumnTypeMetadata_v3 columnTypeMetadata =
            new ColumnTypeMetadata_v3(columnName, col.getType(), colTypeInfo.originalType,
                colTypeInfo.precision, colTypeInfo.scale, colTypeInfo.repetitionLevel, colTypeInfo.definitionLevel);

        if (parquetTableMetadata.columnTypeInfo == null) {
          parquetTableMetadata.columnTypeInfo = new ConcurrentHashMap<>();
        }
        // Save the column schema info. We'll merge it into one list
        parquetTableMetadata.columnTypeInfo
            .put(new ColumnTypeMetadata_v3.Key(columnTypeMetadata.name), columnTypeMetadata);
        if (statsAvailable) {
          // Write stats when they are not null
          Object minValue = null;
          Object maxValue = null;
          if (stats.genericGetMax() != null && stats.genericGetMin() != null ) {
            minValue = stats.genericGetMin();
            maxValue = stats.genericGetMax();
            if (containsCorruptDates == ParquetReaderUtility.DateCorruptionStatus.META_SHOWS_CORRUPTION
                && columnTypeMetadata.originalType == OriginalType.DATE) {
              minValue = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) minValue);
              maxValue = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) maxValue);
            }

          }
          columnMetadata =
              new ColumnMetadata_v3(columnTypeMetadata.name, col.getType(), minValue, maxValue, stats.getNumNulls());
        } else {
          columnMetadata = new ColumnMetadata_v3(columnTypeMetadata.name, col.getType(), null, null, null);
        }
        columnMetadataList.add(columnMetadata);
        length += col.getTotalSize();
      }

      // DRILL-5009: Skip the RowGroup if it is empty
      // Note we still read the schema even if there are no values in the RowGroup
      if (rowGroup.getRowCount() == 0) {
        continue;
      }
      RowGroupMetadata_v3 rowGroupMeta =
          new RowGroupMetadata_v3(rowGroup.getStartingPos(), length, rowGroup.getRowCount(),
              getHostAffinity(file, rowGroup.getStartingPos(), length), columnMetadataList);

      rowGroupMetadataList.add(rowGroupMeta);
    }
    String path = Path.getPathWithoutSchemeAndAuthority(file.getPath()).toString();

    return new ParquetFileMetadata_v3(path, file.getLen(), rowGroupMetadataList);
  }

  /**
   * Get the host affinity for a row group
   *
   * @param fileStatus the parquet file
   * @param start      the start of the row group
   * @param length     the length of the row group
   * @return
   * @throws IOException
   */
  private Map<String, Float> getHostAffinity(FileStatus fileStatus, long start, long length)
      throws IOException {
    BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, start, length);
    Map<String, Float> hostAffinityMap = Maps.newHashMap();
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
   *
   * @param parquetTableMetadata
   * @param p
   * @throws IOException
   */
  private void writeFile(ParquetTableMetadata_v3 parquetTableMetadata, Path p) throws IOException {
    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.configure(Feature.AUTO_CLOSE_TARGET, false);
    jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    ObjectMapper mapper = new ObjectMapper(jsonFactory);
    SimpleModule module = new SimpleModule();
    module.addSerializer(ColumnMetadata_v3.class, new ColumnMetadata_v3.Serializer());
    mapper.registerModule(module);
    FSDataOutputStream os = fs.create(p);
    mapper.writerWithDefaultPrettyPrinter().writeValue(os, parquetTableMetadata);
    os.flush();
    os.close();
  }

  private void writeFile(ParquetTableMetadataDirs parquetTableMetadataDirs, Path p) throws IOException {
    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.configure(Feature.AUTO_CLOSE_TARGET, false);
    jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    ObjectMapper mapper = new ObjectMapper(jsonFactory);
    SimpleModule module = new SimpleModule();
    mapper.registerModule(module);
    FSDataOutputStream os = fs.create(p);
    mapper.writerWithDefaultPrettyPrinter().writeValue(os, parquetTableMetadataDirs);
    os.flush();
    os.close();
  }

  /**
   * Read the parquet metadata from a file
   *
   * @param path
   * @return
   * @throws IOException
   */
  private void readBlockMeta(String path,
      boolean dirsOnly,
      MetadataContext metaContext) throws IOException {
    Stopwatch timer = Stopwatch.createStarted();
    Path p = new Path(path);
    Path parentDir = p.getParent(); // parent directory of the metadata file
    ObjectMapper mapper = new ObjectMapper();

    final SimpleModule serialModule = new SimpleModule();
    serialModule.addDeserializer(SchemaPath.class, new SchemaPath.De());
    serialModule.addKeyDeserializer(ColumnTypeMetadata_v2.Key.class, new ColumnTypeMetadata_v2.Key.DeSerializer());
    serialModule.addKeyDeserializer(ColumnTypeMetadata_v3.Key.class, new ColumnTypeMetadata_v3.Key.DeSerializer());

    AfterburnerModule module = new AfterburnerModule();
    module.setUseOptimizedBeanDeserializer(true);

    mapper.registerModule(serialModule);
    mapper.registerModule(module);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    FSDataInputStream is = fs.open(p);

    boolean alreadyCheckedModification = false;
    boolean newMetadata = false;

    if (metaContext != null) {
      alreadyCheckedModification = metaContext.getStatus(parentDir.toString());
    }

    if (dirsOnly) {
      parquetTableMetadataDirs = mapper.readValue(is, ParquetTableMetadataDirs.class);
      logger.info("Took {} ms to read directories from directory cache file", timer.elapsed(TimeUnit.MILLISECONDS));
      timer.stop();
      if (!alreadyCheckedModification && tableModified(parquetTableMetadataDirs.getDirectories(), p, parentDir, metaContext)) {
        parquetTableMetadataDirs =
            (createMetaFilesRecursively(Path.getPathWithoutSchemeAndAuthority(p.getParent()).toString())).getRight();
        newMetadata = true;
      }
    } else {
      parquetTableMetadata = mapper.readValue(is, ParquetTableMetadataBase.class);
      logger.info("Took {} ms to read metadata from cache file", timer.elapsed(TimeUnit.MILLISECONDS));
      timer.stop();
      if (!alreadyCheckedModification && tableModified(parquetTableMetadata.getDirectories(), p, parentDir, metaContext)) {
        parquetTableMetadata =
            (createMetaFilesRecursively(Path.getPathWithoutSchemeAndAuthority(p.getParent()).toString())).getLeft();
        newMetadata = true;
      }

      // DRILL-5009: Remove the RowGroup if it is empty
      List<? extends ParquetFileMetadata> files = parquetTableMetadata.getFiles();
      for (ParquetFileMetadata file : files) {
        List<? extends RowGroupMetadata> rowGroups = file.getRowGroups();
        for (Iterator<? extends RowGroupMetadata> iter = rowGroups.iterator(); iter.hasNext(); ) {
          RowGroupMetadata r = iter.next();
          if (r.getRowCount() == 0) {
            iter.remove();
          }
        }
      }

    }

    if (newMetadata && metaContext != null) {
      // if new metadata files were created, invalidate the existing metadata context
      metaContext.clear();
    }

  }

  /**
   * Check if the parquet metadata needs to be updated by comparing the modification time of the directories with
   * the modification time of the metadata file
   *
   * @param directories
   * @param metaFilePath
   * @return
   * @throws IOException
   */
  private boolean tableModified(List<String> directories, Path metaFilePath,
      Path parentDir,
      MetadataContext metaContext)
      throws IOException {

    Stopwatch timer = Stopwatch.createStarted();

    if (metaContext != null) {
      metaContext.setStatus(parentDir.toString());
    }
    long metaFileModifyTime = fs.getFileStatus(metaFilePath).getModificationTime();
    FileStatus directoryStatus = fs.getFileStatus(parentDir);
    int numDirs = 1;
    if (directoryStatus.getModificationTime() > metaFileModifyTime) {
      logger.info("Directory {} was modified. Took {} ms to check modification time of {} directories", directoryStatus.getPath().toString(),
          timer.elapsed(TimeUnit.MILLISECONDS),
          numDirs);
      timer.stop();
      return true;
    }
    for (String directory : directories) {
      numDirs++;
      if (metaContext != null) {
        metaContext.setStatus(directory);
      }
      directoryStatus = fs.getFileStatus(new Path(directory));
      if (directoryStatus.getModificationTime() > metaFileModifyTime) {
        logger.info("Directory {} was modified. Took {} ms to check modification time of {} directories", directoryStatus.getPath().toString(),
            timer.elapsed(TimeUnit.MILLISECONDS),
            numDirs);
        timer.stop();
        return true;
      }
    }
    logger.info("No directories were modified. Took {} ms to check modification time of {} directories", timer.elapsed(TimeUnit.MILLISECONDS), numDirs);
    timer.stop();
    return false;
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "metadata_version")
  @JsonSubTypes({
      @JsonSubTypes.Type(value = ParquetTableMetadata_v1.class, name="v1"),
      @JsonSubTypes.Type(value = ParquetTableMetadata_v2.class, name="v2"),
      @JsonSubTypes.Type(value = ParquetTableMetadata_v3.class, name="v3")
      })
  public static abstract class ParquetTableMetadataBase {

    @JsonIgnore public abstract List<String> getDirectories();

    @JsonIgnore public abstract List<? extends ParquetFileMetadata> getFiles();

    @JsonIgnore public abstract void assignFiles(List<? extends ParquetFileMetadata> newFiles);

    public abstract boolean hasColumnMetadata();

    @JsonIgnore public abstract PrimitiveTypeName getPrimitiveType(String[] columnName);

    @JsonIgnore public abstract OriginalType getOriginalType(String[] columnName);

    @JsonIgnore public abstract Integer getRepetitionLevel(String[] columnName);

    @JsonIgnore public abstract Integer getDefinitionLevel(String[] columnName);

    @JsonIgnore public abstract boolean isRowGroupPrunable();

    @JsonIgnore public abstract ParquetTableMetadataBase clone();

    @JsonIgnore public abstract String getDrillVersion();
  }

  public static abstract class ParquetFileMetadata {
    @JsonIgnore public abstract String getPath();

    @JsonIgnore public abstract Long getLength();

    @JsonIgnore public abstract List<? extends RowGroupMetadata> getRowGroups();
  }


  public static abstract class RowGroupMetadata {
    @JsonIgnore public abstract Long getStart();

    @JsonIgnore public abstract Long getLength();

    @JsonIgnore public abstract Long getRowCount();

    @JsonIgnore public abstract Map<String, Float> getHostAffinity();

    @JsonIgnore public abstract List<? extends ColumnMetadata> getColumns();
  }


  public static abstract class ColumnMetadata {
    public abstract String[] getName();

    public abstract Long getNulls();

    public abstract boolean hasSingleValue();

    public abstract Object getMinValue();

    public abstract Object getMaxValue();

    /**
     * Set the max value recorded in the parquet metadata statistics.
     *
     * This object would just be immutable, but due to Drill-4203 we need to correct
     * date values that had been corrupted by earlier versions of Drill.
     */
    public abstract void setMax(Object newMax);

    /**
     * Set the min value recorded in the parquet metadata statistics.
     *
     * This object would just be immutable, but due to Drill-4203 we need to correct
     * date values that had been corrupted by earlier versions of Drill.
     */
    public abstract void setMin(Object newMax);

    public abstract PrimitiveTypeName getPrimitiveType();

    public abstract OriginalType getOriginalType();
  }

  public static class ParquetTableMetadataDirs {
    @JsonProperty List<String> directories;

    public ParquetTableMetadataDirs() {
      // default constructor needed for deserialization
    }

    public ParquetTableMetadataDirs(List<String> directories) {
      this.directories = directories;
    }

    @JsonIgnore public List<String> getDirectories() {
      return directories;
    }

  }

  @JsonTypeName("v1")
  public static class ParquetTableMetadata_v1 extends ParquetTableMetadataBase {
    @JsonProperty List<ParquetFileMetadata_v1> files;
    @JsonProperty List<String> directories;

    public ParquetTableMetadata_v1() {
      super();
    }

    public ParquetTableMetadata_v1(List<ParquetFileMetadata_v1> files, List<String> directories) {
      this.files = files;
      this.directories = directories;
    }

    @JsonIgnore @Override public List<String> getDirectories() {
      return directories;
    }

    @JsonIgnore @Override public List<? extends ParquetFileMetadata> getFiles() {
      return files;
    }

    @JsonIgnore @Override public void assignFiles(List<? extends ParquetFileMetadata> newFiles) {
      this.files = (List<ParquetFileMetadata_v1>) newFiles;
    }

    @Override public boolean hasColumnMetadata() {
      return false;
    }

    @JsonIgnore @Override public PrimitiveTypeName getPrimitiveType(String[] columnName) {
      return null;
    }

    @JsonIgnore @Override public OriginalType getOriginalType(String[] columnName) {
      return null;
    }

    @JsonIgnore @Override
    public Integer getRepetitionLevel(String[] columnName) {
      return null;
    }

    @JsonIgnore @Override
    public Integer getDefinitionLevel(String[] columnName) {
      return null;
    }

    @JsonIgnore @Override
    public boolean isRowGroupPrunable() {
      return false;
    }

    @JsonIgnore @Override public ParquetTableMetadataBase clone() {
      return new ParquetTableMetadata_v1(files, directories);
    }

    @Override
    public String getDrillVersion() {
      return null;
    }
  }


  /**
   * Struct which contains the metadata for a single parquet file
   */
  public static class ParquetFileMetadata_v1 extends ParquetFileMetadata {
    @JsonProperty
    public String path;
    @JsonProperty
    public Long length;
    @JsonProperty
    public List<RowGroupMetadata_v1> rowGroups;

    public ParquetFileMetadata_v1() {
      super();
    }

    public ParquetFileMetadata_v1(String path, Long length, List<RowGroupMetadata_v1> rowGroups) {
      this.path = path;
      this.length = length;
      this.rowGroups = rowGroups;
    }

    @Override
    public String toString() {
      return String.format("path: %s rowGroups: %s", path, rowGroups);
    }

    @JsonIgnore @Override public String getPath() {
      return path;
    }

    @JsonIgnore @Override public Long getLength() {
      return length;
    }

    @JsonIgnore @Override public List<? extends RowGroupMetadata> getRowGroups() {
      return rowGroups;
    }
  }


  /**
   * A struct that contains the metadata for a parquet row group
   */
  public static class RowGroupMetadata_v1 extends RowGroupMetadata {
    @JsonProperty
    public Long start;
    @JsonProperty
    public Long length;
    @JsonProperty
    public Long rowCount;
    @JsonProperty
    public Map<String, Float> hostAffinity;
    @JsonProperty
    public List<ColumnMetadata_v1> columns;

    public RowGroupMetadata_v1() {
      super();
    }

    public RowGroupMetadata_v1(Long start, Long length, Long rowCount, Map<String, Float> hostAffinity,
        List<ColumnMetadata_v1> columns) {
      this.start = start;
      this.length = length;
      this.rowCount = rowCount;
      this.hostAffinity = hostAffinity;
      this.columns = columns;
    }

    @Override public Long getStart() {
      return start;
    }

    @Override public Long getLength() {
      return length;
    }

    @Override public Long getRowCount() {
      return rowCount;
    }

    @Override public Map<String, Float> getHostAffinity() {
      return hostAffinity;
    }

    @Override public List<? extends ColumnMetadata> getColumns() {
      return columns;
    }
  }


  /**
   * A struct that contains the metadata for a column in a parquet file
   */
  public static class ColumnMetadata_v1 extends ColumnMetadata {
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


    public ColumnMetadata_v1() {
      super();
    }

    public ColumnMetadata_v1(SchemaPath name, PrimitiveTypeName primitiveType, OriginalType originalType,
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

    @Override public PrimitiveTypeName getPrimitiveType() {
      return primitiveType;
    }

    @Override public OriginalType getOriginalType() {
      return originalType;
    }

    /**
     * setter used during deserialization of the 'min' field of the metadata cache file.
     *
     * @param min
     */
    @JsonProperty(value = "min")
    public void setMin(Object min) {
      this.min = min;
    }

    /**
     * setter used during deserialization of the 'max' field of the metadata cache file.
     *
     * @param max
     */
    @JsonProperty(value = "max")
    public void setMax(Object max) {
      this.max = max;
    }

    @Override public String[] getName() {
      String[] s = new String[1];
      String nameString = name.toString();
      // Strip out the surrounding backticks.
      s[0]=nameString.substring(1, nameString.length()-1);
      return s;
    }

    @Override public Long getNulls() {
      return nulls;
    }

    @Override public boolean hasSingleValue() {
      return (max != null && min != null && max.equals(min));
    }

    @Override public Object getMinValue() {
      return min;
    }

    @Override public Object getMaxValue() {
      return max;
    }

  }

  /**
   * Struct which contains the metadata for an entire parquet directory structure
   */
  @JsonTypeName("v2") public static class ParquetTableMetadata_v2 extends ParquetTableMetadataBase {
    /*
     ColumnTypeInfo is schema information from all the files and row groups, merged into
     one. To get this info, we pass the ParquetTableMetadata object all the way dow to the
     RowGroup and the column type is built there as it is read from the footer.
     */
    @JsonProperty public ConcurrentHashMap<ColumnTypeMetadata_v2.Key, ColumnTypeMetadata_v2> columnTypeInfo;
    @JsonProperty List<ParquetFileMetadata_v2> files;
    @JsonProperty List<String> directories;
    @JsonProperty String drillVersion;

    public ParquetTableMetadata_v2() {
      super();
    }

    public ParquetTableMetadata_v2(String drillVersion) {
      this.drillVersion = drillVersion;
    }

    public ParquetTableMetadata_v2(ParquetTableMetadataBase parquetTable,
        List<ParquetFileMetadata_v2> files, List<String> directories, String drillVersion) {
      this.files = files;
      this.directories = directories;
      this.columnTypeInfo = ((ParquetTableMetadata_v2) parquetTable).columnTypeInfo;
      this.drillVersion = drillVersion;
    }

    public ParquetTableMetadata_v2(List<ParquetFileMetadata_v2> files, List<String> directories,
        ConcurrentHashMap<ColumnTypeMetadata_v2.Key, ColumnTypeMetadata_v2> columnTypeInfo, String drillVersion) {
      this.files = files;
      this.directories = directories;
      this.columnTypeInfo = columnTypeInfo;
      this.drillVersion = drillVersion;
    }

    public ColumnTypeMetadata_v2 getColumnTypeInfo(String[] name) {
      return columnTypeInfo.get(new ColumnTypeMetadata_v2.Key(name));
    }

    @JsonIgnore @Override public List<String> getDirectories() {
      return directories;
    }

    @JsonIgnore @Override public List<? extends ParquetFileMetadata> getFiles() {
      return files;
    }

    @JsonIgnore @Override public void assignFiles(List<? extends ParquetFileMetadata> newFiles) {
      this.files = (List<ParquetFileMetadata_v2>) newFiles;
    }

    @Override public boolean hasColumnMetadata() {
      return true;
    }

    @JsonIgnore @Override public PrimitiveTypeName getPrimitiveType(String[] columnName) {
      return getColumnTypeInfo(columnName).primitiveType;
    }

    @JsonIgnore @Override public OriginalType getOriginalType(String[] columnName) {
      return getColumnTypeInfo(columnName).originalType;
    }

    @JsonIgnore @Override
    public Integer getRepetitionLevel(String[] columnName) {
      return null;
    }

    @JsonIgnore @Override
    public Integer getDefinitionLevel(String[] columnName) {
      return null;
    }

    @JsonIgnore @Override
    public boolean isRowGroupPrunable() {
      return false;
    }

    @JsonIgnore @Override public ParquetTableMetadataBase clone() {
      return new ParquetTableMetadata_v2(files, directories, columnTypeInfo, drillVersion);
    }

    @JsonIgnore @Override
    public String getDrillVersion() {
      return drillVersion;
    }

  }


  /**
   * Struct which contains the metadata for a single parquet file
   */
  public static class ParquetFileMetadata_v2 extends ParquetFileMetadata {
    @JsonProperty public String path;
    @JsonProperty public Long length;
    @JsonProperty public List<RowGroupMetadata_v2> rowGroups;

    public ParquetFileMetadata_v2() {
      super();
    }

    public ParquetFileMetadata_v2(String path, Long length, List<RowGroupMetadata_v2> rowGroups) {
      this.path = path;
      this.length = length;
      this.rowGroups = rowGroups;
    }

    @Override public String toString() {
      return String.format("path: %s rowGroups: %s", path, rowGroups);
    }

    @JsonIgnore @Override public String getPath() {
      return path;
    }

    @JsonIgnore @Override public Long getLength() {
      return length;
    }

    @JsonIgnore @Override public List<? extends RowGroupMetadata> getRowGroups() {
      return rowGroups;
    }
  }


  /**
   * A struct that contains the metadata for a parquet row group
   */
  public static class RowGroupMetadata_v2 extends RowGroupMetadata {
    @JsonProperty public Long start;
    @JsonProperty public Long length;
    @JsonProperty public Long rowCount;
    @JsonProperty public Map<String, Float> hostAffinity;
    @JsonProperty public List<ColumnMetadata_v2> columns;

    public RowGroupMetadata_v2() {
      super();
    }

    public RowGroupMetadata_v2(Long start, Long length, Long rowCount, Map<String, Float> hostAffinity,
        List<ColumnMetadata_v2> columns) {
      this.start = start;
      this.length = length;
      this.rowCount = rowCount;
      this.hostAffinity = hostAffinity;
      this.columns = columns;
    }

    @Override public Long getStart() {
      return start;
    }

    @Override public Long getLength() {
      return length;
    }

    @Override public Long getRowCount() {
      return rowCount;
    }

    @Override public Map<String, Float> getHostAffinity() {
      return hostAffinity;
    }

    @Override public List<? extends ColumnMetadata> getColumns() {
      return columns;
    }
  }


  public static class ColumnTypeMetadata_v2 {
    @JsonProperty public String[] name;
    @JsonProperty public PrimitiveTypeName primitiveType;
    @JsonProperty public OriginalType originalType;

    // Key to find by name only
    @JsonIgnore private Key key;

    public ColumnTypeMetadata_v2() {
      super();
    }

    public ColumnTypeMetadata_v2(String[] name, PrimitiveTypeName primitiveType, OriginalType originalType) {
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
  public static class ColumnMetadata_v2 extends ColumnMetadata {
    // Use a string array for name instead of Schema Path to make serialization easier
    @JsonProperty public String[] name;
    @JsonProperty public Long nulls;

    public Object mxValue;

    @JsonIgnore private PrimitiveTypeName primitiveType;

    public ColumnMetadata_v2() {
      super();
    }

    public ColumnMetadata_v2(String[] name, PrimitiveTypeName primitiveType, Object mxValue, Long nulls) {
      this.name = name;
      this.mxValue = mxValue;
      this.nulls = nulls;
      this.primitiveType = primitiveType;
    }

    @JsonProperty(value = "mxValue") public void setMax(Object mxValue) {
      this.mxValue = mxValue;
    }

    @Override public String[] getName() {
      return name;
    }

    @Override public Long getNulls() {
      return nulls;
    }

    @Override
    public boolean hasSingleValue() {
      return (mxValue != null);
    }

    @Override public Object getMinValue() {
      return mxValue;
    }

    @Override public Object getMaxValue() {
      return mxValue;
    }

    @Override
    public void setMin(Object newMin) {
      // noop - min value not stored in this version of the metadata
    }

    @Override public PrimitiveTypeName getPrimitiveType() {
      return primitiveType;
    }

    @Override public OriginalType getOriginalType() {
      return null;
    }

    public static class DeSerializer extends JsonDeserializer<ColumnMetadata_v2> {
      @Override public ColumnMetadata_v2 deserialize(JsonParser jp, DeserializationContext ctxt)
          throws IOException, JsonProcessingException {
        return null;
      }
    }


    // We use a custom serializer and write only non null values.
    public static class Serializer extends JsonSerializer<ColumnMetadata_v2> {
      @Override
      public void serialize(ColumnMetadata_v2 value, JsonGenerator jgen, SerializerProvider provider)
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

  /**
   * Struct which contains the metadata for an entire parquet directory structure
   *
   * Difference between v3 and v2 : min/max, type_length, precision, scale, repetitionLevel, definitionLevel
   */
  @JsonTypeName("v3") public static class ParquetTableMetadata_v3 extends ParquetTableMetadataBase {
    /*
     ColumnTypeInfo is schema information from all the files and row groups, merged into
     one. To get this info, we pass the ParquetTableMetadata object all the way dow to the
     RowGroup and the column type is built there as it is read from the footer.
     */
    @JsonProperty public ConcurrentHashMap<ColumnTypeMetadata_v3.Key, ColumnTypeMetadata_v3> columnTypeInfo;
    @JsonProperty List<ParquetFileMetadata_v3> files;
    @JsonProperty List<String> directories;
    @JsonProperty String drillVersion;

    /**
     * Default constructor needed for deserialization from Parquet Metadata Cache Files
     * or for creating an empty instances of this class for the case when the Metadata Cache File is absent
     */
    public ParquetTableMetadata_v3() {
      super();
    }

    /**
     * Used for creating the Parquet Metadata Cache File
     * @param drillVersion  actual version of apache drill
     */
    public ParquetTableMetadata_v3(String drillVersion) {
      this.drillVersion = drillVersion;
    }

    public ParquetTableMetadata_v3(ParquetTableMetadataBase parquetTable,
        List<ParquetFileMetadata_v3> files, List<String> directories, String drillVersion) {
      this.files = files;
      this.directories = directories;
      this.columnTypeInfo = ((ParquetTableMetadata_v3) parquetTable).columnTypeInfo;
      this.drillVersion = drillVersion;
    }

    public ParquetTableMetadata_v3(List<ParquetFileMetadata_v3> files, List<String> directories,
        ConcurrentHashMap<ColumnTypeMetadata_v3.Key, ColumnTypeMetadata_v3> columnTypeInfo,
        String drillVersion) {
      this.files = files;
      this.directories = directories;
      this.columnTypeInfo = columnTypeInfo;
      this.drillVersion = drillVersion;
    }

    public ColumnTypeMetadata_v3 getColumnTypeInfo(String[] name) {
      return columnTypeInfo.get(new ColumnTypeMetadata_v3.Key(name));
    }

    @JsonIgnore @Override public List<String> getDirectories() {
      return directories;
    }

    @JsonIgnore @Override public List<? extends ParquetFileMetadata> getFiles() {
      return files;
    }

    @JsonIgnore @Override public void assignFiles(List<? extends ParquetFileMetadata> newFiles) {
      this.files = (List<ParquetFileMetadata_v3>) newFiles;
    }

    @Override public boolean hasColumnMetadata() {
      return true;
    }

    @JsonIgnore @Override public PrimitiveTypeName getPrimitiveType(String[] columnName) {
      return getColumnTypeInfo(columnName).primitiveType;
    }

    @JsonIgnore @Override public OriginalType getOriginalType(String[] columnName) {
      return getColumnTypeInfo(columnName).originalType;
    }

    @JsonIgnore @Override
    public Integer getRepetitionLevel(String[] columnName) {
      return getColumnTypeInfo(columnName).repetitionLevel;
    }

    @JsonIgnore @Override
    public Integer getDefinitionLevel(String[] columnName) {
      return getColumnTypeInfo(columnName).definitionLevel;
    }

    @JsonIgnore @Override
    public boolean isRowGroupPrunable() {
      return true;
    }

    @JsonIgnore @Override public ParquetTableMetadataBase clone() {
      return new ParquetTableMetadata_v3(files, directories, columnTypeInfo, drillVersion);
    }

    @JsonIgnore @Override
    public String getDrillVersion() {
      return drillVersion;
    }

  }


  /**
   * Struct which contains the metadata for a single parquet file
   */
  public static class ParquetFileMetadata_v3 extends ParquetFileMetadata {
    @JsonProperty public String path;
    @JsonProperty public Long length;
    @JsonProperty public List<RowGroupMetadata_v3> rowGroups;

    public ParquetFileMetadata_v3() {
      super();
    }

    public ParquetFileMetadata_v3(String path, Long length, List<RowGroupMetadata_v3> rowGroups) {
      this.path = path;
      this.length = length;
      this.rowGroups = rowGroups;
    }

    @Override public String toString() {
      return String.format("path: %s rowGroups: %s", path, rowGroups);
    }

    @JsonIgnore @Override public String getPath() {
      return path;
    }

    @JsonIgnore @Override public Long getLength() {
      return length;
    }

    @JsonIgnore @Override public List<? extends RowGroupMetadata> getRowGroups() {
      return rowGroups;
    }
  }


  /**
   * A struct that contains the metadata for a parquet row group
   */
  public static class RowGroupMetadata_v3 extends RowGroupMetadata {
    @JsonProperty public Long start;
    @JsonProperty public Long length;
    @JsonProperty public Long rowCount;
    @JsonProperty public Map<String, Float> hostAffinity;
    @JsonProperty public List<ColumnMetadata_v3> columns;

    public RowGroupMetadata_v3() {
      super();
    }

    public RowGroupMetadata_v3(Long start, Long length, Long rowCount, Map<String, Float> hostAffinity,
        List<ColumnMetadata_v3> columns) {
      this.start = start;
      this.length = length;
      this.rowCount = rowCount;
      this.hostAffinity = hostAffinity;
      this.columns = columns;
    }

    @Override public Long getStart() {
      return start;
    }

    @Override public Long getLength() {
      return length;
    }

    @Override public Long getRowCount() {
      return rowCount;
    }

    @Override public Map<String, Float> getHostAffinity() {
      return hostAffinity;
    }

    @Override public List<? extends ColumnMetadata> getColumns() {
      return columns;
    }
  }


  public static class ColumnTypeMetadata_v3 {
    @JsonProperty public String[] name;
    @JsonProperty public PrimitiveTypeName primitiveType;
    @JsonProperty public OriginalType originalType;
    @JsonProperty public int precision;
    @JsonProperty public int scale;
    @JsonProperty public int repetitionLevel;
    @JsonProperty public int definitionLevel;

    // Key to find by name only
    @JsonIgnore private Key key;

    public ColumnTypeMetadata_v3() {
      super();
    }

    public ColumnTypeMetadata_v3(String[] name, PrimitiveTypeName primitiveType, OriginalType originalType, int precision, int scale, int repetitionLevel, int definitionLevel) {
      this.name = name;
      this.primitiveType = primitiveType;
      this.originalType = originalType;
      this.precision = precision;
      this.scale = scale;
      this.repetitionLevel = repetitionLevel;
      this.definitionLevel = definitionLevel;
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
  public static class ColumnMetadata_v3 extends ColumnMetadata {
    // Use a string array for name instead of Schema Path to make serialization easier
    @JsonProperty public String[] name;
    @JsonProperty public Long nulls;

    public Object minValue;
    public Object maxValue;

    @JsonIgnore private PrimitiveTypeName primitiveType;

    public ColumnMetadata_v3() {
      super();
    }

    public ColumnMetadata_v3(String[] name, PrimitiveTypeName primitiveType, Object minValue, Object maxValue, Long nulls) {
      this.name = name;
      this.minValue = minValue;
      this.maxValue = maxValue;
      this.nulls = nulls;
      this.primitiveType = primitiveType;
    }

    @JsonProperty(value = "minValue") public void setMin(Object minValue) {
      this.minValue = minValue;
    }

    @JsonProperty(value = "maxValue") public void setMax(Object maxValue) {
      this.maxValue = maxValue;
    }

    @Override public String[] getName() {
      return name;
    }

    @Override public Long getNulls() {
      return nulls;
    }

    @Override
    public boolean hasSingleValue() {
      return (minValue !=null && maxValue != null && minValue.equals(maxValue));
    }

    @Override public Object getMinValue() {
      return minValue;
    }

    @Override public Object getMaxValue() {
      return maxValue;
    }

    @Override public PrimitiveTypeName getPrimitiveType() {
      return null;
    }

    @Override public OriginalType getOriginalType() {
      return null;
    }

    public static class DeSerializer extends JsonDeserializer<ColumnMetadata_v3> {
      @Override public ColumnMetadata_v3 deserialize(JsonParser jp, DeserializationContext ctxt)
          throws IOException, JsonProcessingException {
        return null;
      }
    }


    // We use a custom serializer and write only non null values.
    public static class Serializer extends JsonSerializer<ColumnMetadata_v3> {
      @Override
      public void serialize(ColumnMetadata_v3 value, JsonGenerator jgen, SerializerProvider provider)
          throws IOException, JsonProcessingException {
        jgen.writeStartObject();
        jgen.writeArrayFieldStart("name");
        for (String n : value.name) {
          jgen.writeString(n);
        }
        jgen.writeEndArray();
        if (value.minValue != null) {
          Object val;
          if (value.primitiveType == PrimitiveTypeName.BINARY && value.minValue != null) {
            val = new String(((Binary) value.minValue).getBytes());
          } else {
            val = value.minValue;
          }
          jgen.writeObjectField("minValue", val);
        }
        if (value.maxValue != null) {
          Object val;
          if (value.primitiveType == PrimitiveTypeName.BINARY && value.maxValue != null) {
            val = new String(((Binary) value.maxValue).getBytes());
          } else {
            val = value.maxValue;
          }
          jgen.writeObjectField("maxValue", val);
        }

        if (value.nulls != null) {
          jgen.writeObjectField("nulls", value.nulls);
        }
        jgen.writeEndObject();
      }
    }

  }

}

