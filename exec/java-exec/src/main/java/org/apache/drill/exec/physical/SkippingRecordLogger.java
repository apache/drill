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
package org.apache.drill.exec.physical;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.skiprecord.RecordContextVisitor;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.tukaani.xz.UnsupportedOptionsException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SkippingRecordLogger {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SkippingRecordLogger.class);

  private final RecordBatch incoming;
  private final List<Pair<Integer, String>> exprList;
  private final Map<String, String> recordInfo;
  private final String qid;
  private final String minorFrag;
  private final String address;

  private int numSkippedRecords;

  private final FileSystem fs;
  private final String logDestination;
  private FSDataOutputStream os;
  private JsonFactory jsonFactory;

  public static SkippingRecordLogger createLogger(FragmentContext context, RecordBatch incoming) throws ExecutionSetupException {
    if(!context.getOptions().getOption(ExecConstants.ENABLE_SKIP_INVALID_RECORD)) {
      return null;
    }

    try {
      final String dfs_name = context.getOptions().getOption(ExecConstants.SKIP_INVALID_RECORD_DFS);
      final String work_space_name = context.getOptions().getOption(ExecConstants.SKIP_INVALID_RECORD_WORKSPACE);

      final StoragePlugin storagePlugin = context.getDrillbitContext().getStorage().getPlugin(dfs_name);
      if (!(storagePlugin instanceof FileSystemPlugin)) {
        throw new UnsupportedOperationException();
      }
      final FileSystemPlugin fileSystemPlugin = (FileSystemPlugin) storagePlugin;
      final DrillFileSystem fs = new DrillFileSystem(fileSystemPlugin.getFsConf());

      String root = null;
      for(Map.Entry<String, WorkspaceConfig> entry : ((FileSystemConfig) fileSystemPlugin.getConfig()).workspaces.entrySet()) {
        if(entry.getKey().toUpperCase().equals(work_space_name.toUpperCase())) {
          final WorkspaceConfig skip = entry.getValue();
          if(!skip.isWritable()) {
            throw new IOException("The selected workspace `" + dfs_name + '.' + work_space_name + "` is not writable");
          }

          root = skip.getLocation();
          break;
        }
      }

      if(root == null) {
        throw new IOException("The destination for logging offending records needs to be registered under distributed file system storage plugin");
      }

      if(root.charAt(root.length() - 1) == '/') {
        root = root.substring(0, root.length() - 1);
      }

      final String subroot;
      final String qid = QueryIdHelper.getQueryId(context.getHandle().getQueryId());
      final String logFileName = context.getOptions().getOption(ExecConstants.SKIP_INVALID_RECORD_LOG_NAME);
      if(logFileName.isEmpty()) {
        subroot = qid;
      } else {
        subroot = logFileName; // + "/" + qid;
      }
      final String minorFrag = context.getHandle().getMajorFragmentId() + "-" + context.getHandle().getMinorFragmentId();
      final String address = context.getDrillbitContext().getEndpoint().getAddress();
      return new SkippingRecordLogger(
          incoming,
          fs,
          root + "/" + subroot + "/" + minorFrag + ".json",
          qid,
          minorFrag,
          address);
    } catch (IOException ioe) {
      throw UserException
        .dataWriteError(ioe)
        .build(logger);
    } catch (Exception e) {
      throw new ExecutionSetupException(e.getCause());
    }
  }

  private SkippingRecordLogger(RecordBatch incoming, FileSystem fs, String path, String qid, String minorFrag, String address) throws IOException {
    this.incoming = incoming;
    this.exprList = Lists.newArrayList();
    this.recordInfo = Maps.newHashMap();
    this.qid = qid;
    this.minorFrag = minorFrag;
    this.address = address;

    this.fs = fs;
    this.numSkippedRecords = 0;
    this.logDestination = path;
  }

  public void addExpr(Pair<Integer, String> pair) {
    exprList.add(pair);
  }

  public final void append(String key, String str) {
    recordInfo.put(key, str);
  }

  public final void appendContext(final int index) {
    for(MaterializedField materializedField : incoming.getSchema()) {
      final SchemaPath schemaPath = materializedField.getPath();
      if(schemaPath.getRootSegment().getPath().startsWith(RecordContextVisitor.VIRTUAL_COLUMN_PREFIX)) {
        final TypedFieldId typedFieldId = incoming.getValueVectorId(schemaPath);
        final VarCharVector varCharVector =
            (VarCharVector) incoming.getValueAccessorById(VarCharVector.class, typedFieldId.getFieldIds()).getValueVector();

        final String column = schemaPath.getRootSegment().getPath().substring(
            schemaPath.getRootSegment().getPath().indexOf(RecordContextVisitor.VIRTUAL_COLUMN_PREFIX) + 1);

        append(column, varCharVector.getAccessor().getObject(index).toString());
        append("Query_ID", qid);
        append("Fragment_ID", minorFrag);
        append("Drillbit_Address", address);
      }
    }
  }

  public void incrementNumSkippedRecord() {
    ++numSkippedRecords;
  }

  public int getNumSkippedRecordAndClear() {
    final int tmp = numSkippedRecords;
    numSkippedRecords = 0;
    return tmp;
  }

  public List<Pair<Integer, String>> getExprList() {
    return exprList;
  }

  public void write() throws IOException {
    if(os == null) {
      final Path p = new Path(logDestination);
      os = fs.create(p);
      jsonFactory = new JsonFactory();
      jsonFactory.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
      jsonFactory.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    }

    final ObjectMapper mapper = new ObjectMapper(jsonFactory);
    final Map<String, String> map = Maps.newHashMap();
    for(Map.Entry<String, String> entry : recordInfo.entrySet()) {
      map.put(entry.getKey(), entry.getValue());
    }
    recordInfo.clear();
    mapper.writerWithDefaultPrettyPrinter().writeValue(os, map);
  }

  public void flush() {
    if(os == null) {
      return;
    }

    try {
      os.flush();
      os.close();
    } catch (IOException e) {
      logger.error("Cannot write to log: {}", e);
    }
  }
}