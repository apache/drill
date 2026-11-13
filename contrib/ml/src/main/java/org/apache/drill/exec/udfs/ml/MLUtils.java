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
package org.apache.drill.exec.udfs.ml;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.pmml4s.data.Series;
import org.pmml4s.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;

public class MLUtils {

  private static final Logger logger = LoggerFactory.getLogger(MLUtils.class);

  // Anything that looks like scheme://... is a URL. Models are never fetched from URLs.
  private static final Pattern URL_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9+.-]*://.*");

  /**
   * Loads a {@link Model} from a PMML file in a file system storage plugin that Drill can access.
   * The location has the form {@code <plugin>.<workspace>.<path>} (or {@code <plugin>.<path>}
   * for the default workspace). URLs are not accepted.
   *
   * @param modelLocation The location of the model file.
   * @param context A {@link DrillbitContext}
   * @return The Model contained in the file referenced by modelLocation
   */
  public static Model getModel(String modelLocation, DrillbitContext context) {
    if (modelLocation == null || modelLocation.trim().isEmpty()) {
      throw UserException.functionError()
        .message("A model location is required.")
        .build(logger);
    }
    if (URL_PATTERN.matcher(modelLocation.trim()).matches()) {
      throw UserException.functionError()
        .message("Models cannot be loaded from URLs. Use a file system storage plugin location "
          + "such as dfs.models.`model.pmml`. Location: " + modelLocation)
        .build(logger);
    }
    return getModelFromFile(modelLocation, context);
  }

  private static Model getModelFromFile(String modelLocation, DrillbitContext context) {
    String location = modelLocation.replace("`", "");
    String pluginInstanceName = location.split("\\.", 2)[0];
    StoragePlugin plugin;
    try {
      plugin = context.getStorage().getPlugin(pluginInstanceName);
    } catch (PluginException e) {
      throw UserException.functionError(e)
        .message("Unable to access storage plugin '" + pluginInstanceName + "' for model " + modelLocation)
        .build(logger);
    }
    if (!(plugin instanceof FileSystemPlugin)) {
      throw UserException.functionError()
        .message("The model location must name a file system storage plugin, but '"
          + pluginInstanceName + "' is not one. Location: " + modelLocation)
        .build(logger);
    }

    FileSystemPlugin fileSystemPlugin = (FileSystemPlugin) plugin;
    String fullFilePath = getFullFilePath(location, (FileSystemConfig) fileSystemPlugin.getConfig());

    try (DrillFileSystem dfs = new DrillFileSystem(fileSystemPlugin.getFsConf());
         FSDataInputStream inputStream = dfs.open(new Path(fullFilePath))) {
      return Model.fromInputStream(inputStream);
    } catch (IOException e) {
      throw UserException.functionError(e)
        .message("Unable to open model file " + fullFilePath + " (" + modelLocation + "): " + e.getMessage())
        .build(logger);
    } catch (RuntimeException e) {
      // pmml4s signals an unparseable or unsupported model with unchecked exceptions
      throw UserException.functionError(e)
        .message("Unable to read model file " + fullFilePath + " (" + modelLocation
          + ") as a PMML model: " + e.getMessage())
        .build(logger);
    }
  }

  /**
   * Resolves a backtick-free {@code plugin.workspace.path} or {@code plugin.path} string to a
   * file system path.
   */
  protected static String getFullFilePath(String location, FileSystemConfig config) {
    String[] parts = location.replace("`", "").split("\\.", 3);
    String fileSystemName = parts[0];
    Map<String, WorkspaceConfig> workspaces = config.getWorkspaces();

    if (parts.length == 3 && workspaces.containsKey(parts[1])) {
      // plugin.workspace.path
      return join(workspaces.get(parts[1]).getLocation(), parts[2]);
    }

    // plugin.path, where the path itself may contain dots
    String fileName = location.replace("`", "").substring(fileSystemName.length() + 1);
    if (workspaces.isEmpty()) {
      // Plugins without workspaces, such as the classpath plugin
      return fileName;
    }
    WorkspaceConfig defaultWorkspace = workspaces.get("default");
    return defaultWorkspace == null ? fileName : join(defaultWorkspace.getLocation(), fileName);
  }

  private static String join(String prefix, String fileName) {
    if (prefix == null || prefix.isEmpty()) {
      return fileName;
    }
    return prefix.endsWith("/") ? prefix + fileName : prefix + "/" + fileName;
  }

  /**
   * Returns a prediction from a machine learning model. The model must be derived from a
   * PMML file and stored in a {@link Model} object. Returns a Map of probabilities and other
   * attributes of the prediction.
   *
   * @param model The trained model.
   * @param featureHolders An array of {@link FieldReader} containing observations
   * @return A Map of the predicted classes or values as well as the probabilities.
   */
  public static Map<String, Object> predict(Model model, FieldReader[] featureHolders) {
    if (model.inputFields().length != featureHolders.length) {
      throw UserException.functionError()
        .message("Model expecting " + model.inputFields().length + " features, but query provides " + featureHolders.length)
        .build(logger);
    }

    Object[] features = new Object[featureHolders.length];
    for (int i = 0; i < featureHolders.length; i++) {
      FieldReader currentReader = featureHolders[i];
      MinorType dataType = currentReader.getType().getMinorType();
      if (dataType == MinorType.VARCHAR) {
        features[i] = currentReader.readText().toString();
      } else if (dataType == MinorType.VARDECIMAL) {
        features[i] = currentReader.readBigDecimal().doubleValue();
      } else if (dataType == MinorType.FLOAT4) {
        features[i] = currentReader.readFloat().doubleValue();
      } else if (dataType == MinorType.FLOAT8) {
        features[i] = currentReader.readDouble();
      } else if (dataType == MinorType.INT) {
        features[i] = currentReader.readInteger().doubleValue();
      } else if (dataType == MinorType.BIGINT) {
        features[i] = currentReader.readLong().doubleValue();
      } else {
        throw UserException.functionError()
          .message("Does not support " + dataType.name() + " in models. "
            + currentReader.getField().getName() + " is a " + dataType.name())
          .build(logger);
      }
    }

    return model.predict(Series.fromArray(features)).asJavaMap();
  }

  /**
   * Writes the prediction map to the output map writer. Doubles become FLOAT8 and strings
   * VARCHAR; other value types are skipped.
   */
  public static DrillBuf mapFields(Map<String, Object> predictions, ComplexWriter outWriter, DrillBuf buffer) {
    MapWriter queryMapWriter = outWriter.rootAsMap();

    if (predictions == null || predictions.isEmpty()) {
      queryMapWriter.start();
      queryMapWriter.end();
      return buffer;
    }

    for (Map.Entry<String, Object> field : predictions.entrySet()) {
      String fieldName = field.getKey();
      Object value = field.getValue();

      if (value instanceof Double) {
        queryMapWriter.float8(fieldName).writeFloat8((Double) value);
      } else if (value instanceof String) {
        VarCharHolder fieldHolder = new VarCharHolder();
        byte[] rowStringBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
        buffer = buffer.reallocIfNeeded(rowStringBytes.length);
        buffer.setBytes(0, rowStringBytes);

        fieldHolder.start = 0;
        fieldHolder.end = rowStringBytes.length;
        fieldHolder.buffer = buffer;

        queryMapWriter.varChar(fieldName).write(fieldHolder);
      }
    }

    return buffer;
  }
}
