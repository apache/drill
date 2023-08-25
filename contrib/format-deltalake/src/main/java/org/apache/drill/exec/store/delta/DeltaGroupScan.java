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
package org.apache.drill.exec.store.delta;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.types.ArrayType;
import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.DateType;
import io.delta.standalone.types.DecimalType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.ShortType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;
import org.apache.calcite.util.Pair;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.expr.StatisticsProvider;
import org.apache.drill.exec.expr.fn.impl.DateUtility;
import org.apache.drill.exec.metastore.MetadataProviderManager;
import org.apache.drill.exec.metastore.store.FileSystemMetadataProviderManager;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.delta.format.DeltaFormatPlugin;
import org.apache.drill.exec.store.delta.plan.DrillExprToDeltaTranslator;
import org.apache.drill.exec.store.delta.snapshot.DeltaSnapshotFactory;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSelection;
import org.apache.drill.exec.store.dfs.ReadEntryWithPath;
import org.apache.drill.exec.store.parquet.AbstractParquetGroupScan;
import org.apache.drill.exec.store.parquet.ParquetReaderConfig;
import org.apache.drill.exec.store.parquet.RowGroupReadEntry;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.metastore.metadata.LocationProvider;
import org.apache.drill.metastore.metadata.Metadata;
import org.apache.drill.metastore.statistics.ColumnStatistics;
import org.apache.drill.metastore.statistics.TableStatisticsKind;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@JsonTypeName("delta-scan")
public class DeltaGroupScan extends AbstractParquetGroupScan {

  private final DeltaFormatPlugin formatPlugin;

  private final String path;

  private final TupleMetadata schema;

  private final LogicalExpression condition;

  private final DrillFileSystem fs;

  private List<AddFile> addFiles;

  private List<EndpointAffinity> endpointAffinities;

  private final Map<Path, Map<String, String>> partitionHolder;

  @JsonCreator
  public DeltaGroupScan(
    @JsonProperty("userName") String userName,
    @JsonProperty("entries") List<ReadEntryWithPath> entries,
    @JsonProperty("storage") StoragePluginConfig storageConfig,
    @JsonProperty("format") FormatPluginConfig formatConfig,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("schema") TupleMetadata schema,
    @JsonProperty("path") String path,
    @JsonProperty("readerConfig") ParquetReaderConfig readerConfig,
    @JsonProperty("condition") LogicalExpression condition,
    @JsonProperty("limit") Integer limit,
    @JsonProperty("partitionHolder") Map<Path, Map<String, String>> partitionHolder,
    @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException {
    super(ImpersonationUtil.resolveUserName(userName), columns, entries, readerConfig, condition);
    this.formatPlugin = pluginRegistry.resolveFormat(storageConfig, formatConfig, DeltaFormatPlugin.class);
    this.columns = columns;
    this.path = path;
    this.schema = schema;
    this.condition = condition;
    this.limit = limit;
    this.fs = ImpersonationUtil.createFileSystem(
      ImpersonationUtil.resolveUserName(userName), formatPlugin.getFsConf());

    DeltaParquetTableMetadataProvider metadataProvider =
      defaultTableMetadataProviderBuilder(new FileSystemMetadataProviderManager())
        .withEntries(entries)
        .withFormatPlugin(formatPlugin)
        .withReaderConfig(readerConfig)
        .withSchema(schema)
        .build();

    this.metadataProvider = metadataProvider;
    this.entries = metadataProvider.getEntries();
    this.partitionHolder = partitionHolder;
    this.fileSet = metadataProvider.getFileSet();

    init();
  }

  private DeltaGroupScan(DeltaGroupScanBuilder builder) throws IOException {
    super(ImpersonationUtil.resolveUserName(builder.userName), builder.columns,
      builder.entries, builder.readerConfig, builder.condition);
    this.formatPlugin = builder.formatPlugin;
    this.columns = builder.columns;
    this.path = builder.path;
    this.schema = builder.schema;
    this.condition = builder.condition;
    this.limit = builder.limit;
    this.fs = ImpersonationUtil.createFileSystem(
      ImpersonationUtil.resolveUserName(userName), formatPlugin.getFsConf());

    DeltaParquetTableMetadataProvider metadataProvider =
      defaultTableMetadataProviderBuilder(new FileSystemMetadataProviderManager())
        .withEntries(entries)
        .withFormatPlugin(formatPlugin)
        .withReaderConfig(readerConfig)
        .withSchema(schema)
        .build();

    this.metadataProvider = metadataProvider;
    this.entries = metadataProvider.getEntries();
    this.partitionHolder = builder.partitionValues;
    this.fileSet = metadataProvider.getFileSet();

    init();
  }

  /**
   * Private constructor, used for cloning.
   *
   * @param that The DeltaGroupScan to clone
   */
  private DeltaGroupScan(DeltaGroupScan that) {
    super(that);
    this.columns = that.columns;
    this.formatPlugin = that.formatPlugin;
    this.path = that.path;
    this.condition = that.condition;
    this.schema = that.schema;
    this.mappings = that.mappings;
    this.fs = that.fs;
    this.limit = that.limit;
    this.addFiles = that.addFiles;
    this.endpointAffinities = that.endpointAffinities;
    this.partitionHolder = that.partitionHolder;
  }

  @Override
  protected DeltaParquetTableMetadataProvider.Builder tableMetadataProviderBuilder(MetadataProviderManager source) {
    return defaultTableMetadataProviderBuilder(source);
  }

  @Override
  protected DeltaParquetTableMetadataProvider.Builder defaultTableMetadataProviderBuilder(MetadataProviderManager source) {
    return new DeltaParquetTableMetadataProvider.Builder(source);
  }

  public static DeltaGroupScanBuilder builder() {
    return new DeltaGroupScanBuilder();
  }

  @Override
  public DeltaGroupScan clone(List<SchemaPath> columns) {
    DeltaGroupScan groupScan = new DeltaGroupScan(this);
    groupScan.columns = columns;
    return groupScan;
  }

  @Override
  public DeltaGroupScan applyLimit(int maxRecords) {
    DeltaGroupScan clone = new DeltaGroupScan(this);
    clone.limit = maxRecords;
    return clone;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) {
    List<RowGroupReadEntry> readEntries = getReadEntries(minorFragmentId);
    Map<Path, Map<String, String>> subPartitionHolder = new HashMap<>();
    for (RowGroupReadEntry readEntry : readEntries) {
      Map<String, String> values = partitionHolder.get(readEntry.getPath());
      subPartitionHolder.put(readEntry.getPath(), values);
    }
    return new DeltaRowGroupScan(getUserName(), formatPlugin, readEntries, columns, subPartitionHolder,
      readerConfig, filter, getTableMetadata().getSchema());
  }

  @Override
  public DeltaGroupScan clone(FileSelection selection) throws IOException {
    DeltaGroupScan newScan = new DeltaGroupScan(this);
    newScan.modifyFileSelection(selection);
    newScan.init();
    return newScan;
  }

  @Override
  protected RowGroupScanFilterer<?> getFilterer() {
    return new DeltaParquetScanFilterer(this);
  }

  @Override
  protected Collection<DrillbitEndpoint> getDrillbits() {
    return formatPlugin.getContext().getBits();
  }

  @Override
  protected AbstractParquetGroupScan cloneWithFileSelection(Collection<Path> filePaths) throws IOException {
    FileSelection newSelection = new FileSelection(null, new ArrayList<>(filePaths), null, null, false);
    return clone(newSelection);
  }

  @Override
  protected boolean supportsFileImplicitColumns() {
    // current group scan should populate directory partition values
    return false;
  }

  @Override
  protected List<String> getPartitionValues(LocationProvider locationProvider) {
    return Collections.emptyList();
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new DeltaGroupScan(this);
  }

  @Override
  public boolean supportsLimitPushdown() {
    return false;
  }

  @JsonProperty("schema")
  public TupleMetadata getSchema() {
    return schema;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getStorageConfig() {
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty("format")
  public FormatPluginConfig getFormatConfig() {
    return formatPlugin.getConfig();
  }

  @JsonProperty("path")
  public String getPath() {
    return path;
  }

  @JsonProperty("condition")
  public LogicalExpression getCondition() {
    return condition;
  }

  @JsonProperty("partitionHolder")
  public Map<Path, Map<String, String>> getPartitionHolder() {
    return partitionHolder;
  }

  @Override
  public String toString() {
    return new PlanStringBuilder(this)
      .field("path", path)
      .field("entries", entries)
      .field("schema", schema)
      .field("columns", columns)
      .field("addFiles", addFiles)
      .field("limit", limit)
      .field("numFiles", getEntries().size())
      .toString();
  }

  private static class DeltaParquetScanFilterer extends RowGroupScanFilterer<DeltaParquetScanFilterer> {

    public DeltaParquetScanFilterer(DeltaGroupScan source) {
      super(source);
    }

    @Override
    protected AbstractParquetGroupScan getNewScan() {
      return new DeltaGroupScan((DeltaGroupScan) source);
    }

    @Override
    protected DeltaParquetScanFilterer self() {
      return this;
    }

    @Override
    protected  <T extends Metadata> Map<SchemaPath, ColumnStatistics<?>> getImplicitColumnStatistics(
      OptionManager optionManager, T metadata, Map<SchemaPath, ColumnStatistics<?>> columnsStatistics) {
      if (metadata instanceof LocationProvider && optionManager != null) {
        LocationProvider locationProvider = (LocationProvider) metadata;
        columnsStatistics = new HashMap<>(columnsStatistics);
        Map<String, String> partitions =
          ((DeltaGroupScan) source).getPartitionHolder().get(locationProvider.getPath());
        for (Map.Entry<String, String> partitionValue : partitions.entrySet()) {
          TypeProtos.MinorType minorType =
            tableSchema.column(partitionValue.getKey()).getType().getMinorType();
          String value = partitionValue.getValue();
          if (value != null) {
            columnsStatistics.put(SchemaPath.getCompoundPath(partitionValue.getKey()),
              StatisticsProvider.getConstantColumnStatistics(
                castPartitionValue(value, minorType), minorType));
          } else {
            Long rowCount = TableStatisticsKind.ROW_COUNT.getValue(metadata);
            columnsStatistics.put(SchemaPath.getCompoundPath(partitionValue.getKey()),
              StatisticsProvider.getColumnStatistics(null, null, rowCount, minorType));
          }
        }
      }

      return columnsStatistics;
    }

    private Object castPartitionValue(String value, TypeProtos.MinorType type) {
      switch (type) {
      case BIT:
        return Boolean.parseBoolean(value);
      case TINYINT:
        return Byte.parseByte(value);
      case SMALLINT:
        return Short.parseShort(value);
      case INT:
        return Integer.parseInt(value);
      case BIGINT:
        return Long.parseLong(value);
      case FLOAT4:
        return Float.parseFloat(value);
      case FLOAT8:
        return Double.parseDouble(value);
      case DATE:
        return DateUtility.parseLocalDate(value);
      case TIME:
        return DateUtility.parseLocalTime(value);
      case TIMESTAMP:
        return DateUtility.parseBest(value);
      case VARCHAR:
        return value;
      case VARDECIMAL:
        return new BigDecimal(value);
      default:
        throw new UnsupportedOperationException("Unsupported partition type: " + type);
      }
    }
  }

  public static class DeltaGroupScanBuilder {
    private String userName;

    private DeltaFormatPlugin formatPlugin;

    private TupleMetadata schema;

    private String path;

    private LogicalExpression condition;

    private List<SchemaPath> columns;

    private int limit;

    private List<ReadEntryWithPath> entries;

    private ParquetReaderConfig readerConfig = ParquetReaderConfig.getDefaultInstance();

    private Map<Path, Map<String, String>> partitionValues;

    public DeltaGroupScanBuilder userName(String userName) {
      this.userName = userName;
      return this;
    }

    public DeltaGroupScanBuilder formatPlugin(DeltaFormatPlugin formatPlugin) {
      this.formatPlugin = formatPlugin;
      return this;
    }

    public DeltaGroupScanBuilder schema(TupleMetadata schema) {
      this.schema = schema;
      return this;
    }

    public DeltaGroupScanBuilder path(String path) {
      this.path = path;
      return this;
    }

    public DeltaGroupScanBuilder condition(LogicalExpression condition) {
      this.condition = condition;
      return this;
    }

    public DeltaGroupScanBuilder columns(List<SchemaPath> columns) {
      this.columns = columns;
      return this;
    }

    public DeltaGroupScanBuilder limit(int maxRecords) {
      this.limit = maxRecords;
      return this;
    }

    public DeltaGroupScanBuilder readerConfig(ParquetReaderConfig readerConfig) {
      this.readerConfig = readerConfig;
      return this;
    }

    public DeltaGroupScan build() throws IOException {
      DeltaLog log = DeltaLog.forTable(formatPlugin.getFsConf(), path);
      DeltaSnapshotFactory.SnapshotContext context = DeltaSnapshotFactory.SnapshotContext.builder()
        .snapshotAsOfTimestamp(formatPlugin.getConfig().getTimestamp())
        .snapshotAsOfVersion(formatPlugin.getConfig().getVersion())
        .build();
      Snapshot snapshot = DeltaSnapshotFactory.INSTANCE.create(context).apply(log);
      StructType structType = snapshot.getMetadata().getSchema();
      schema = toSchema(structType);

      DeltaScan scan = Optional.ofNullable(condition)
        .map(c -> c.accept(new DrillExprToDeltaTranslator(structType), null))
        .map(snapshot::scan)
        .orElse(snapshot.scan());

      try {
        CloseableIterator<AddFile> files = scan.getFiles();
        ArrayList<AddFile> addFiles = Lists.newArrayList(() -> files);
        entries = addFiles.stream()
          .map(addFile -> new ReadEntryWithPath(new Path(URI.create(path).getPath(), URI.create(addFile.getPath()).getPath())))
          .collect(Collectors.toList());

        partitionValues = addFiles.stream()
          .collect(Collectors.toMap(
            addFile -> new Path(URI.create(path).getPath(), URI.create(addFile.getPath()).getPath()),
            addFile -> collectPartitionedValues(snapshot, addFile)));

        files.close();
      } catch (IOException e) {
        throw new DrillRuntimeException(e);
      }

      return new DeltaGroupScan(this);
    }

    private Map<String, String> collectPartitionedValues(Snapshot snapshot, AddFile addFile) {
      Map<String, String> partitionValues = new LinkedHashMap<>();
      snapshot.getMetadata().getPartitionColumns().stream()
        .map(col -> Pair.of(col, addFile.getPartitionValues().get(col)))
        .forEach(pair -> partitionValues.put(pair.getKey(), pair.getValue()));
      return partitionValues;
    }

    private TupleMetadata toSchema(StructType structType) {
      TupleBuilder tupleBuilder = new TupleBuilder();
      for (StructField field : structType.getFields()) {
        tupleBuilder.addColumn(toColumnMetadata(field));
      }

      return tupleBuilder.schema();
    }

    private ColumnMetadata toColumnMetadata(StructField field) {
      DataType dataType = field.getDataType();
      if (dataType instanceof ArrayType) {
        DataType elementType = ((ArrayType) dataType).getElementType();
        if (elementType instanceof ArrayType) {
          return MetadataUtils.newRepeatedList(field.getName(),
            toColumnMetadata(new StructField(field.getName(), ((ArrayType) elementType).getElementType(), false)));
        } else if (elementType instanceof StructType) {
          return MetadataUtils.newMapArray(field.getName(), toSchema((StructType) elementType));
        }
        return MetadataUtils.newScalar(field.getName(), toMinorType(elementType), TypeProtos.DataMode.REPEATED);
      } else if (dataType instanceof StructType) {
        return MetadataUtils.newMap(field.getName(), toSchema((StructType) dataType));
      } else {
        return MetadataUtils.newScalar(field.getName(), toMinorType(field.getDataType()),
          field.isNullable() ? TypeProtos.DataMode.OPTIONAL : TypeProtos.DataMode.REQUIRED);
      }
    }

    private TypeProtos.MinorType toMinorType(DataType dataType) {
      if (dataType instanceof BinaryType) {
        return TypeProtos.MinorType.VARBINARY;
      } else if (dataType instanceof BooleanType) {
        return TypeProtos.MinorType.BIT;
      } else if (dataType instanceof ByteType) {
        return TypeProtos.MinorType.TINYINT;
      } else if (dataType instanceof DateType) {
        return TypeProtos.MinorType.DATE;
      } else if (dataType instanceof DecimalType) {
        return TypeProtos.MinorType.VARDECIMAL;
      } else if (dataType instanceof DoubleType) {
        return TypeProtos.MinorType.FLOAT8;
      } else if (dataType instanceof FloatType) {
        return TypeProtos.MinorType.FLOAT4;
      } else if (dataType instanceof IntegerType) {
        return TypeProtos.MinorType.INT;
      } else if (dataType instanceof LongType) {
        return TypeProtos.MinorType.BIGINT;
      } else if (dataType instanceof ShortType) {
        return TypeProtos.MinorType.SMALLINT;
      } else if (dataType instanceof StringType) {
        return TypeProtos.MinorType.VARCHAR;
      } else if (dataType instanceof TimestampType) {
        return TypeProtos.MinorType.TIMESTAMP;
      } else {
        throw new DrillRuntimeException("Unsupported data type: " + dataType);
      }
    }
  }
}
