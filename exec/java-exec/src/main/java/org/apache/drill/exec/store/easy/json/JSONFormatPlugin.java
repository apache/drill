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
package org.apache.drill.exec.store.easy.json;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.QueryContext.SqlStatementType;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileReaderFactory;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileScanBuilder;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.planner.common.DrillStatsTable;
import org.apache.drill.exec.planner.common.DrillStatsTable.TableStatistics;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.StatisticsRecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.easy.json.JSONFormatPlugin.JSONFormatConfig;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl.JsonOptions;
import org.apache.drill.shaded.guava.com.google.common.base.Objects;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONFormatPlugin extends EasyFormatPlugin<JSONFormatConfig> {
  private static final Logger logger = LoggerFactory.getLogger(JSONFormatPlugin.class);
  public static final String PLUGIN_NAME = "json";
  private static final String DEFAULT_EXTN = "json";
  private static final boolean IS_COMPRESSIBLE = true;
  public static class JsonReaderCreator extends FileReaderFactory {

    private final JsonOptions options;

    public JsonReaderCreator(JsonOptions options) {
      this.options = options;
    }

    @Override
    public ManagedReader<? extends FileSchemaNegotiator> newReader() {
      return new JsonBatchReader(options);
    }
  }

  public JSONFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig storageConfig) {
    this(name, context, fsConf, storageConfig, new JSONFormatConfig());
  }

  public JSONFormatPlugin(String name, DrillbitContext context, Configuration fsConf,
      StoragePluginConfig config, JSONFormatConfig formatPluginConfig) {
    super(name, easyConfig(fsConf, formatPluginConfig), context, config, formatPluginConfig);
  }

  private static EasyFormatConfig easyConfig(Configuration fsConf, JSONFormatConfig pluginConfig) {
    EasyFormatConfig config = new EasyFormatConfig();
    config.readable = true;
    config.writable = true;
    config.blockSplittable = false;
    config.compressible = IS_COMPRESSIBLE;
    config.supportsProjectPushdown = true;
    config.extensions = pluginConfig.getExtensions();
    config.fsConf = fsConf;
    config.defaultName = PLUGIN_NAME;
    config.readerOperatorType = CoreOperatorType.JSON_SUB_SCAN_VALUE;
    config.writerOperatorType = CoreOperatorType.JSON_WRITER_VALUE;
    // Temporary until V2 is the default.
    // config.useEnhancedScan = true;
    config.supportsStatistics = true;
    return config;
  }

  @Override
  public RecordReader getRecordReader(FragmentContext context,
                                      DrillFileSystem dfs,
                                      FileWork fileWork,
                                      List<SchemaPath> columns,
                                      String userName) {
    return new JSONRecordReader(context, fileWork.getPath(), dfs, columns);
  }

  @Override
  public boolean isStatisticsRecordWriter(FragmentContext context, EasyWriter writer) {
    return context.getSQLStatementType() == SqlStatementType.ANALYZE;
  }

  @Override
  public StatisticsRecordWriter getStatisticsRecordWriter(FragmentContext context, EasyWriter writer)
      throws IOException {
    StatisticsRecordWriter recordWriter;
    //ANALYZE statement requires the special statistics writer
    if (!isStatisticsRecordWriter(context, writer)) {
      return null;
    }
    Map<String, String> options = setupOptions(context, writer, true);
    recordWriter = new JsonStatisticsRecordWriter(getFsConf(), this);
    recordWriter.init(options);
    return recordWriter;
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
    RecordWriter recordWriter;
    Map<String, String> options = setupOptions(context, writer, true);
    recordWriter = new JsonRecordWriter(writer.getStorageStrategy(), getFsConf());
    recordWriter.init(options);
    return recordWriter;
  }

  private Map<String, String> setupOptions(FragmentContext context, EasyWriter writer, boolean statsOptions) {
    Map<String, String> options = new HashMap<>();
    options.put("location", writer.getLocation());

    OptionManager optionMgr = context.getOptions();
    FragmentHandle handle = context.getHandle();
    String fragmentId = String.format("%d_%d", handle.getMajorFragmentId(), handle.getMinorFragmentId());
    options.put("prefix", fragmentId);
    options.put("separator", " ");
    options.put("extension", "json");
    options.put("extended", Boolean.toString(optionMgr.getBoolean(ExecConstants.JSON_EXTENDED_TYPES_KEY)));
    options.put("uglify", Boolean.toString(optionMgr.getBoolean(ExecConstants.JSON_WRITER_UGLIFY_KEY)));
    options.put("skipnulls", Boolean.toString(optionMgr.getBoolean(ExecConstants.JSON_WRITER_SKIPNULLFIELDS_KEY)));
    options.put("enableNanInf", Boolean.toString(optionMgr.getBoolean(ExecConstants.JSON_WRITER_NAN_INF_NUMBERS)));
    if (statsOptions) {
      options.put("queryid", context.getQueryIdString());
    }
    return options;
  }

  @Override
  public void writeStatistics(TableStatistics statistics, FileSystem fs, Path statsTablePath) throws IOException {
    FSDataOutputStream stream = null;
    JsonGenerator generator = null;
    try {
      JsonFactory factory = new JsonFactory();
      stream = fs.create(statsTablePath);
      ObjectMapper mapper = DrillStatsTable.getMapper();
      generator = factory.createGenerator((OutputStream) stream).useDefaultPrettyPrinter().setCodec(mapper);
      mapper.writeValue(generator, statistics);
    } catch (com.fasterxml.jackson.core.JsonGenerationException ex) {
      logger.error("Unable to create file (JSON generation error): " + statsTablePath.getName(), ex);
      throw ex;
    } catch (com.fasterxml.jackson.databind.JsonMappingException ex) {
      logger.error("Unable to create file (JSON mapping error): " + statsTablePath.getName(), ex);
      throw ex;
    } catch (IOException ex) {
      logger.error("Unable to create file " + statsTablePath.getName(), ex);
    } finally {
      if (generator != null) {
        generator.flush();
      }
      if (stream != null) {
        stream.close();
      }
    }
  }

  @Override
  protected boolean useEnhancedScan(OptionManager options) {
    // Create the "legacy", "V1" reader or the new "V2" version based on
    // the result set loader. The V2 version is a bit more robust, and
    // supports the row set framework. However, V1 supports unions.
    // This code should be temporary.
    return options.getBoolean(ExecConstants.ENABLE_V2_JSON_READER_KEY);
  }

  @Override
  protected FileScanBuilder frameworkBuilder(
      OptionManager options, EasySubScan scan) throws ExecutionSetupException {
    FileScanBuilder builder = new FileScanBuilder();
    initScanBuilder(builder, scan);
    builder.setReaderFactory(new JsonReaderCreator(null));

    // Project missing columns as Varchar, which is at least
    // compatible with all-text mode. (JSON never returns a nullable
    // int, so don't use the default.)

    builder.setNullType(Types.optional(MinorType.VARCHAR));

    return builder;
  }

  @JsonTypeName(PLUGIN_NAME)
  public static class JSONFormatConfig implements FormatPluginConfig {

    private static final List<String> DEFAULT_EXTS = ImmutableList.of(DEFAULT_EXTN);
    public List<String> extensions = DEFAULT_EXTS;

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public List<String> getExtensions() {
      // when loading an old JSONFormatConfig that doesn't contain an "extensions" attribute
      return extensions == null ? DEFAULT_EXTS : extensions;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(extensions);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      JSONFormatConfig other = (JSONFormatConfig) obj;
      return Objects.equal(extensions, other.extensions);
    }
  }
}
