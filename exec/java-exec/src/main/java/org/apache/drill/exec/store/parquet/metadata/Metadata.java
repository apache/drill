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
package org.apache.drill.exec.store.parquet.metadata;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator.Feature;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import org.apache.drill.exec.store.parquet.ParquetReaderConfig;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.collections.Collectors;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.DrillVersionInfo;
import org.apache.drill.exec.store.TimedCallable;
import org.apache.drill.exec.store.dfs.MetadataContext;
import org.apache.drill.exec.store.parquet.ParquetReaderUtility;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;
import static org.apache.drill.exec.store.parquet.metadata.MetadataBase.ParquetFileMetadata;
import static org.apache.drill.exec.store.parquet.metadata.MetadataBase.ParquetTableMetadataBase;
import static org.apache.drill.exec.store.parquet.metadata.MetadataBase.RowGroupMetadata;
import static org.apache.drill.exec.store.parquet.metadata.MetadataVersion.Constants.SUPPORTED_VERSIONS;
import static org.apache.drill.exec.store.parquet.metadata.Metadata_V3.ColumnMetadata_v3;
import static org.apache.drill.exec.store.parquet.metadata.Metadata_V3.ColumnTypeMetadata_v3;
import static org.apache.drill.exec.store.parquet.metadata.Metadata_V3.ParquetFileMetadata_v3;
import static org.apache.drill.exec.store.parquet.metadata.Metadata_V3.ParquetTableMetadata_v3;
import static org.apache.drill.exec.store.parquet.metadata.Metadata_V3.RowGroupMetadata_v3;

public class Metadata {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Metadata.class);

  public static final String[] OLD_METADATA_FILENAMES = {".drill.parquet_metadata.v2"};
  public static final String METADATA_FILENAME = ".drill.parquet_metadata";
  public static final String METADATA_DIRECTORIES_FILENAME = ".drill.parquet_metadata_directories";

  private final ParquetReaderConfig readerConfig;

  private ParquetTableMetadataBase parquetTableMetadata;
  private ParquetTableMetadataDirs parquetTableMetadataDirs;


  private Metadata(ParquetReaderConfig readerConfig) {
    this.readerConfig = readerConfig;
  }

  /**
   * Create the parquet metadata file for the directory at the given path, and for any subdirectories.
   *
   * @param fs file system
   * @param path path
   * @param readerConfig parquet reader configuration
   */
  public static void createMeta(FileSystem fs, String path, ParquetReaderConfig readerConfig) throws IOException {
    Metadata metadata = new Metadata(readerConfig);
    metadata.createMetaFilesRecursively(path, fs);
  }

  /**
   * Get the parquet metadata for the parquet files in the given directory, including those in subdirectories.
   *
   * @param fs file system
   * @param path path
   * @param readerConfig parquet reader configuration
   *
   * @return parquet table metadata
   */
  public static ParquetTableMetadata_v3 getParquetTableMetadata(FileSystem fs, String path, ParquetReaderConfig readerConfig) throws IOException {
    Metadata metadata = new Metadata(readerConfig);
    return metadata.getParquetTableMetadata(path, fs);
  }

  /**
   * Get the parquet metadata for a list of parquet files.
   *
   * @param fileStatusMap file statuses and corresponding file systems
   * @param readerConfig parquet reader configuration
   * @return parquet table metadata
   */
  public static ParquetTableMetadata_v3 getParquetTableMetadata(Map<FileStatus, FileSystem> fileStatusMap,
                                                                ParquetReaderConfig readerConfig) throws IOException {
    Metadata metadata = new Metadata(readerConfig);
    return metadata.getParquetTableMetadata(fileStatusMap);
  }

  /**
   * Get the parquet metadata for the table by reading the metadata file
   *
   * @param fs current file system
   * @param path The path to the metadata file, located in the directory that contains the parquet files
   * @param metaContext metadata context
   * @param readerConfig parquet reader configuration
   * @return parquet table metadata. Null if metadata cache is missing, unsupported or corrupted
   */
  public static @Nullable ParquetTableMetadataBase readBlockMeta(FileSystem fs,
                                                                 Path path,
                                                                 MetadataContext metaContext,
                                                                 ParquetReaderConfig readerConfig) {
    if (ignoreReadingMetadata(metaContext, path)) {
      return null;
    }
    Metadata metadata = new Metadata(readerConfig);
    metadata.readBlockMeta(path, false, metaContext, fs);
    return metadata.parquetTableMetadata;
  }

  /**
   * Get the parquet metadata for all subdirectories by reading the metadata file
   *
   * @param fs current file system
   * @param path The path to the metadata file, located in the directory that contains the parquet files
   * @param metaContext metadata context
   * @param readerConfig parquet reader configuration
   * @return parquet metadata for a directory. Null if metadata cache is missing, unsupported or corrupted
   */
  public static @Nullable ParquetTableMetadataDirs readMetadataDirs(FileSystem fs,
                                                                    Path path,
                                                                    MetadataContext metaContext,
                                                                    ParquetReaderConfig readerConfig) {
    if (ignoreReadingMetadata(metaContext, path)) {
      return null;
    }
    Metadata metadata = new Metadata(readerConfig);
    metadata.readBlockMeta(path, true, metaContext, fs);
    return metadata.parquetTableMetadataDirs;
  }

  /**
   * Ignore reading metadata files, if metadata is missing, unsupported or corrupted
   *
   * @param metaContext Metadata context
   * @param path The path to the metadata file, located in the directory that contains the parquet files
   * @return true if parquet metadata is missing or corrupted, false otherwise
   */
  private static boolean ignoreReadingMetadata(MetadataContext metaContext, Path path) {
    if (metaContext.isMetadataCacheCorrupted()) {
      logger.warn("Ignoring of reading '{}' metadata file. Parquet metadata cache files are unsupported or corrupted. " +
          "Query performance may be slow. Make sure the cache files are up-to-date by running the 'REFRESH TABLE " +
          "METADATA' command", path);
      return true;
    }
    return false;
  }

  /**
   * Create the parquet metadata files for the directory at the given path and for any subdirectories.
   * Metadata cache files written to the disk contain relative paths. Returned Pair of metadata contains absolute paths.
   *
   * @param path to the directory of the parquet table
   * @param fs file system
   * @return Pair of parquet metadata. The left one is a parquet metadata for the table. The right one of the Pair is
   *         a metadata for all subdirectories (if they are present and there are no any parquet files in the
   *         {@code path} directory).
   * @throws IOException if parquet metadata can't be serialized and written to the json file
   */
  private Pair<ParquetTableMetadata_v3, ParquetTableMetadataDirs> createMetaFilesRecursively(final String path, FileSystem fs) throws IOException {
    Stopwatch timer = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;
    List<ParquetFileMetadata_v3> metaDataList = Lists.newArrayList();
    List<String> directoryList = Lists.newArrayList();
    ConcurrentHashMap<ColumnTypeMetadata_v3.Key, ColumnTypeMetadata_v3> columnTypeInfoSet =
        new ConcurrentHashMap<>();
    Path p = new Path(path);
    FileStatus fileStatus = fs.getFileStatus(p);
    assert fileStatus.isDirectory() : "Expected directory";

    final Map<FileStatus, FileSystem> childFiles = new LinkedHashMap<>();

    for (final FileStatus file : DrillFileSystemUtil.listAll(fs, p, false)) {
      if (file.isDirectory()) {
        ParquetTableMetadata_v3 subTableMetadata = (createMetaFilesRecursively(file.getPath().toString(), fs)).getLeft();
        metaDataList.addAll(subTableMetadata.files);
        directoryList.addAll(subTableMetadata.directories);
        directoryList.add(file.getPath().toString());
        // Merge the schema from the child level into the current level
        //TODO: We need a merge method that merges two columns with the same name but different types
        columnTypeInfoSet.putAll(subTableMetadata.columnTypeInfo);
      } else {
        childFiles.put(file, fs);
      }
    }
    ParquetTableMetadata_v3 parquetTableMetadata = new ParquetTableMetadata_v3(SUPPORTED_VERSIONS.last().toString(),
                                                                                DrillVersionInfo.getVersion());
    if (childFiles.size() > 0) {
      List<ParquetFileMetadata_v3 > childFilesMetadata = getParquetFileMetadata_v3(parquetTableMetadata, childFiles);
      metaDataList.addAll(childFilesMetadata);
      // Note that we do not need to merge the columnInfo at this point. The columnInfo is already added
      // to the parquetTableMetadata.
    }

    parquetTableMetadata.directories = directoryList;
    parquetTableMetadata.files = metaDataList;
    // TODO: We need a merge method that merges two columns with the same name but different types
    if (parquetTableMetadata.columnTypeInfo == null) {
      parquetTableMetadata.columnTypeInfo = new ConcurrentHashMap<>();
    }
    parquetTableMetadata.columnTypeInfo.putAll(columnTypeInfoSet);

    for (String oldName : OLD_METADATA_FILENAMES) {
      fs.delete(new Path(p, oldName), false);
    }
    //  relative paths in the metadata are only necessary for meta cache files.
    ParquetTableMetadata_v3 metadataTableWithRelativePaths =
        MetadataPathUtils.createMetadataWithRelativePaths(parquetTableMetadata, path);
    writeFile(metadataTableWithRelativePaths, new Path(p, METADATA_FILENAME), fs);

    if (directoryList.size() > 0 && childFiles.size() == 0) {
      ParquetTableMetadataDirs parquetTableMetadataDirsRelativePaths =
          new ParquetTableMetadataDirs(metadataTableWithRelativePaths.directories);
      writeFile(parquetTableMetadataDirsRelativePaths, new Path(p, METADATA_DIRECTORIES_FILENAME), fs);
      if (timer != null) {
        logger.debug("Creating metadata files recursively took {} ms", timer.elapsed(TimeUnit.MILLISECONDS));
      }
      ParquetTableMetadataDirs parquetTableMetadataDirs = new ParquetTableMetadataDirs(directoryList);
      return Pair.of(parquetTableMetadata, parquetTableMetadataDirs);
    }
    List<String> emptyDirList = Lists.newArrayList();
    if (timer != null) {
      logger.debug("Creating metadata files recursively took {} ms", timer.elapsed(TimeUnit.MILLISECONDS));
      timer.stop();
    }
    return Pair.of(parquetTableMetadata, new ParquetTableMetadataDirs(emptyDirList));
  }

  /**
   * Get the parquet metadata for the parquet files in a directory.
   *
   * @param path the path of the directory
   * @return metadata object for an entire parquet directory structure
   * @throws IOException in case of problems during accessing files
   */
  private ParquetTableMetadata_v3 getParquetTableMetadata(String path, FileSystem fs) throws IOException {
    Path p = new Path(path);
    FileStatus fileStatus = fs.getFileStatus(p);
    Stopwatch watch = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;
    List<FileStatus> fileStatuses = new ArrayList<>();
    if (fileStatus.isFile()) {
      fileStatuses.add(fileStatus);
    } else {
      fileStatuses.addAll(DrillFileSystemUtil.listFiles(fs, p, true));
    }
    if (watch != null) {
      logger.debug("Took {} ms to get file statuses", watch.elapsed(TimeUnit.MILLISECONDS));
      watch.reset();
      watch.start();
    }

    Map<FileStatus, FileSystem> fileStatusMap = fileStatuses.stream()
        .collect(
            java.util.stream.Collectors.toMap(
                Function.identity(),
                s -> fs,
                (oldFs, newFs) -> newFs,
                LinkedHashMap::new));

    ParquetTableMetadata_v3 metadata_v3 = getParquetTableMetadata(fileStatusMap);
    if (watch != null) {
      logger.debug("Took {} ms to read file metadata", watch.elapsed(TimeUnit.MILLISECONDS));
      watch.stop();
    }
    return metadata_v3;
  }

  /**
   * Get the parquet metadata for a list of parquet files
   *
   * @param fileStatusMap file statuses and corresponding file systems
   * @return parquet table metadata object
   * @throws IOException if parquet file metadata can't be obtained
   */
  private ParquetTableMetadata_v3 getParquetTableMetadata(Map<FileStatus, FileSystem> fileStatusMap)
      throws IOException {
    ParquetTableMetadata_v3 tableMetadata = new ParquetTableMetadata_v3(SUPPORTED_VERSIONS.last().toString(),
                                                                        DrillVersionInfo.getVersion());
    tableMetadata.files = getParquetFileMetadata_v3(tableMetadata, fileStatusMap);
    tableMetadata.directories = new ArrayList<>();
    return tableMetadata;
  }

  /**
   * Get a list of file metadata for a list of parquet files
   *
   * @param parquetTableMetadata_v3 can store column schema info from all the files and row groups
   * @param fileStatusMap parquet files statuses and corresponding file systems
   *
   * @return list of the parquet file metadata with absolute paths
   * @throws IOException is thrown in case of issues while executing the list of runnables
   */
  private List<ParquetFileMetadata_v3> getParquetFileMetadata_v3(
      ParquetTableMetadata_v3 parquetTableMetadata_v3, Map<FileStatus, FileSystem> fileStatusMap) throws IOException {
    return TimedCallable.run("Fetch parquet metadata", logger,
        Collectors.toList(fileStatusMap,
            (fileStatus, fileSystem) -> new MetadataGatherer(parquetTableMetadata_v3, fileStatus, fileSystem)),
        16
    );
  }

  /**
   * TimedRunnable that reads the footer from parquet and collects file metadata
   */
  private class MetadataGatherer extends TimedCallable<ParquetFileMetadata_v3> {

    private final ParquetTableMetadata_v3 parquetTableMetadata;
    private final FileStatus fileStatus;
    private final FileSystem fs;

    MetadataGatherer(ParquetTableMetadata_v3 parquetTableMetadata, FileStatus fileStatus, FileSystem fs) {
      this.parquetTableMetadata = parquetTableMetadata;
      this.fileStatus = fileStatus;
      this.fs = fs;
    }

    @Override
    protected ParquetFileMetadata_v3 runInner() throws Exception {
      return getParquetFileMetadata_v3(parquetTableMetadata, fileStatus, fs);
    }

    public String toString() {
      return new ToStringBuilder(this, SHORT_PREFIX_STYLE).append("path", fileStatus.getPath()).toString();
    }
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

    ColTypeInfo(OriginalType originalType, int precision, int scale, int repetitionLevel, int definitionLevel) {
      this.originalType = originalType;
      this.precision = precision;
      this.scale = scale;
      this.repetitionLevel = repetitionLevel;
      this.definitionLevel = definitionLevel;
    }
  }

  /**
   * Get the metadata for a single file
   */
  private ParquetFileMetadata_v3 getParquetFileMetadata_v3(ParquetTableMetadata_v3 parquetTableMetadata,
      final FileStatus file, final FileSystem fs) throws IOException, InterruptedException {
    final ParquetMetadata metadata;
    final UserGroupInformation processUserUgi = ImpersonationUtil.getProcessUserUGI();
    final Configuration conf = new Configuration(fs.getConf());
    try {
      metadata = processUserUgi.doAs((PrivilegedExceptionAction<ParquetMetadata>)() -> {
        try (ParquetFileReader parquetFileReader = ParquetFileReader.open(HadoopInputFile.fromStatus(file, conf), readerConfig.toReadOptions())) {
          return parquetFileReader.getFooter();
        }
      });
    } catch(Exception e) {
      logger.error("Exception while reading footer of parquet file [Details - path: {}, owner: {}] as process user {}",
        file.getPath(), file.getOwner(), processUserUgi.getShortUserName(), e);
      throw e;
    }

    MessageType schema = metadata.getFileMetaData().getSchema();

    Map<SchemaPath, ColTypeInfo> colTypeInfoMap = new HashMap<>();
    schema.getPaths();
    for (String[] path : schema.getPaths()) {
      colTypeInfoMap.put(SchemaPath.getCompoundPath(path), getColTypeInfo(schema, schema, path, 0));
    }

    List<RowGroupMetadata_v3> rowGroupMetadataList = Lists.newArrayList();

    ArrayList<SchemaPath> ALL_COLS = new ArrayList<>();
    ALL_COLS.add(SchemaPath.STAR_COLUMN);
    ParquetReaderUtility.DateCorruptionStatus containsCorruptDates = ParquetReaderUtility.detectCorruptDates(metadata, ALL_COLS,
      readerConfig.autoCorrectCorruptedDates());
    logger.debug("Contains corrupt dates: {}.", containsCorruptDates);

    for (BlockMetaData rowGroup : metadata.getBlocks()) {
      List<ColumnMetadata_v3> columnMetadataList = new ArrayList<>();
      long length = 0;
      for (ColumnChunkMetaData col : rowGroup.getColumns()) {
        Statistics<?> stats = col.getStatistics();
        String[] columnName = col.getPath().toArray();
        SchemaPath columnSchemaName = SchemaPath.getCompoundPath(columnName);
        ColTypeInfo colTypeInfo = colTypeInfoMap.get(columnSchemaName);

        ColumnTypeMetadata_v3 columnTypeMetadata =
            new ColumnTypeMetadata_v3(columnName, col.getPrimitiveType().getPrimitiveTypeName(), colTypeInfo.originalType,
                colTypeInfo.precision, colTypeInfo.scale, colTypeInfo.repetitionLevel, colTypeInfo.definitionLevel);

        if (parquetTableMetadata.columnTypeInfo == null) {
          parquetTableMetadata.columnTypeInfo = new ConcurrentHashMap<>();
        }
        parquetTableMetadata.columnTypeInfo.put(new ColumnTypeMetadata_v3.Key(columnTypeMetadata.name), columnTypeMetadata);

        // Save the column schema info. We'll merge it into one list
        Object minValue = null;
        Object maxValue = null;
        long numNulls = -1;
        boolean statsAvailable = stats != null && !stats.isEmpty();
        if (statsAvailable) {
          if (stats.hasNonNullValue()) {
            minValue = stats.genericGetMin();
            maxValue = stats.genericGetMax();
            if (containsCorruptDates == ParquetReaderUtility.DateCorruptionStatus.META_SHOWS_CORRUPTION && columnTypeMetadata.originalType == OriginalType.DATE) {
              minValue = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) minValue);
              maxValue = ParquetReaderUtility.autoCorrectCorruptedDate((Integer) maxValue);
            }
          }
          numNulls = stats.getNumNulls();
        }
        ColumnMetadata_v3 columnMetadata = new ColumnMetadata_v3(columnTypeMetadata.name, col.getPrimitiveType().getPrimitiveTypeName(), minValue, maxValue, numNulls);
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
              getHostAffinity(file, fs, rowGroup.getStartingPos(), length), columnMetadataList);

      rowGroupMetadataList.add(rowGroupMeta);
    }
    String path = Path.getPathWithoutSchemeAndAuthority(file.getPath()).toString();

    return new ParquetFileMetadata_v3(path, file.getLen(), rowGroupMetadataList);
  }

  /**
   * Get the host affinity for a row group.
   *
   * @param fileStatus the parquet file
   * @param start      the start of the row group
   * @param length     the length of the row group
   * @return host affinity for the row group
   */
  private Map<String, Float> getHostAffinity(FileStatus fileStatus, FileSystem fs, long start, long length)
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
   * Serialize parquet metadata to json and write to a file.
   *
   * @param parquetTableMetadata parquet table metadata
   * @param p file path
   */
  private void writeFile(ParquetTableMetadata_v3 parquetTableMetadata, Path p, FileSystem fs) throws IOException {
    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.configure(Feature.AUTO_CLOSE_TARGET, false);
    jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    ObjectMapper mapper = new ObjectMapper(jsonFactory);
    SimpleModule module = new SimpleModule();
    module.addSerializer(ColumnMetadata_v3.class, new ColumnMetadata_v3.Serializer());
    mapper.registerModule(module);
    OutputStream os = fs.create(p);
    mapper.writerWithDefaultPrettyPrinter().writeValue(os, parquetTableMetadata);
    os.flush();
    os.close();
  }

  private void writeFile(ParquetTableMetadataDirs parquetTableMetadataDirs, Path p, FileSystem fs) throws IOException {
    JsonFactory jsonFactory = new JsonFactory();
    jsonFactory.configure(Feature.AUTO_CLOSE_TARGET, false);
    jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    ObjectMapper mapper = new ObjectMapper(jsonFactory);
    SimpleModule module = new SimpleModule();
    mapper.registerModule(module);
    OutputStream os = fs.create(p);
    mapper.writerWithDefaultPrettyPrinter().writeValue(os, parquetTableMetadataDirs);
    os.flush();
    os.close();
  }

  /**
   * Read the parquet metadata from a file
   *
   * @param path to metadata file
   * @param dirsOnly true for {@link Metadata#METADATA_DIRECTORIES_FILENAME}
   *                 or false for {@link Metadata#METADATA_FILENAME} files reading
   * @param metaContext current metadata context
   */
  private void readBlockMeta(Path path, boolean dirsOnly, MetadataContext metaContext, FileSystem fs) {
    Stopwatch timer = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;
    Path metadataParentDir = Path.getPathWithoutSchemeAndAuthority(path.getParent());
    String metadataParentDirPath = metadataParentDir.toUri().getPath();
    ObjectMapper mapper = new ObjectMapper();

    final SimpleModule serialModule = new SimpleModule();
    serialModule.addDeserializer(SchemaPath.class, new SchemaPath.De());
    serialModule.addKeyDeserializer(Metadata_V2.ColumnTypeMetadata_v2.Key.class, new Metadata_V2.ColumnTypeMetadata_v2.Key.DeSerializer());
    serialModule.addKeyDeserializer(ColumnTypeMetadata_v3.Key.class, new ColumnTypeMetadata_v3.Key.DeSerializer());

    AfterburnerModule module = new AfterburnerModule();
    module.setUseOptimizedBeanDeserializer(true);

    mapper.registerModule(serialModule);
    mapper.registerModule(module);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    try (InputStream is = fs.open(path)) {
      boolean alreadyCheckedModification;
      boolean newMetadata = false;
      alreadyCheckedModification = metaContext.getStatus(metadataParentDirPath);

      if (dirsOnly) {
        parquetTableMetadataDirs = mapper.readValue(is, ParquetTableMetadataDirs.class);
        if (timer != null) {
          logger.debug("Took {} ms to read directories from directory cache file", timer.elapsed(TimeUnit.MILLISECONDS));
          timer.stop();
        }
        parquetTableMetadataDirs.updateRelativePaths(metadataParentDirPath);
        if (!alreadyCheckedModification && tableModified(parquetTableMetadataDirs.getDirectories(), path, metadataParentDir, metaContext, fs)) {
          parquetTableMetadataDirs =
              (createMetaFilesRecursively(Path.getPathWithoutSchemeAndAuthority(path.getParent()).toString(), fs)).getRight();
          newMetadata = true;
        }
      } else {
        parquetTableMetadata = mapper.readValue(is, ParquetTableMetadataBase.class);
        if (timer != null) {
          logger.debug("Took {} ms to read metadata from cache file", timer.elapsed(TimeUnit.MILLISECONDS));
          timer.stop();
        }
        if (new MetadataVersion(parquetTableMetadata.getMetadataVersion()).compareTo(new MetadataVersion(3, 0)) >= 0) {
          ((ParquetTableMetadata_v3) parquetTableMetadata).updateRelativePaths(metadataParentDirPath);
        }
        if (!alreadyCheckedModification && tableModified(parquetTableMetadata.getDirectories(), path, metadataParentDir, metaContext, fs)) {
          parquetTableMetadata =
              (createMetaFilesRecursively(Path.getPathWithoutSchemeAndAuthority(path.getParent()).toString(), fs)).getLeft();
          newMetadata = true;
        }

        // DRILL-5009: Remove the RowGroup if it is empty
        List<? extends ParquetFileMetadata> files = parquetTableMetadata.getFiles();
        for (ParquetFileMetadata file : files) {
          List<? extends RowGroupMetadata> rowGroups = file.getRowGroups();
          rowGroups.removeIf(r -> r.getRowCount() == 0);
        }

      }
      if (newMetadata) {
        // if new metadata files were created, invalidate the existing metadata context
        metaContext.clear();
      }
    } catch (IOException e) {
      logger.error("Failed to read '{}' metadata file", path, e);
      metaContext.setMetadataCacheCorrupted(true);
    }
  }

  /**
   * Check if the parquet metadata needs to be updated by comparing the modification time of the directories with
   * the modification time of the metadata file
   *
   * @param directories List of directories
   * @param metaFilePath path of parquet metadata cache file
   * @return true if metadata needs to be updated, false otherwise
   * @throws IOException if some resources are not accessible
   */
  private boolean tableModified(List<String> directories, Path metaFilePath, Path parentDir, MetadataContext metaContext, FileSystem fs) throws IOException {
    Stopwatch timer = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;
    metaContext.setStatus(parentDir.toUri().getPath());
    long metaFileModifyTime = fs.getFileStatus(metaFilePath).getModificationTime();
    FileStatus directoryStatus = fs.getFileStatus(parentDir);
    int numDirs = 1;
    if (directoryStatus.getModificationTime() > metaFileModifyTime) {
      if (timer != null) {
        logger.debug("Directory {} was modified. Took {} ms to check modification time of {} directories",
            directoryStatus.getPath().toString(), timer.elapsed(TimeUnit.MILLISECONDS), numDirs);
        timer.stop();
      }
      return true;
    }
    for (String directory : directories) {
      numDirs++;
      metaContext.setStatus(directory);
      directoryStatus = fs.getFileStatus(new Path(directory));
      if (directoryStatus.getModificationTime() > metaFileModifyTime) {
        if (timer != null) {
          logger.debug("Directory {} was modified. Took {} ms to check modification time of {} directories",
              directoryStatus.getPath().toString(), timer.elapsed(TimeUnit.MILLISECONDS), numDirs);
          timer.stop();
        }
        return true;
      }
    }
    if (timer != null) {
      logger.debug("No directories were modified. Took {} ms to check modification time of {} directories",
          timer.elapsed(TimeUnit.MILLISECONDS), numDirs);
      timer.stop();
    }
    return false;
  }

}

