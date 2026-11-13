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
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.StoragePlugin;
import org.apache.drill.exec.store.StoragePluginRegistry.PluginException;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.WorkspaceConfig;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.pmml4s.data.Series;
import org.pmml4s.model.Model;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
   * @param queryUser The user running the query. Under impersonation the model file is read
   *     as this user, so {@code predict()} cannot reach a file the user could not select.
   * @return The Model contained in the file referenced by modelLocation
   */
  public static Model getModel(String modelLocation, DrillbitContext context, String queryUser) {
    if (modelLocation == null || modelLocation.trim().isEmpty()) {
      throw UserException.functionError()
        .message("A model location is required.")
        .build(logger);
    }
    if (URL_PATTERN.matcher(modelLocation.replace("`", "").trim()).matches()) {
      throw UserException.functionError()
        .message("Models cannot be loaded from URLs. Use a file system storage plugin location "
          + "such as dfs.models.`model.pmml`. Location: " + modelLocation)
        .build(logger);
    }
    return getModelFromFile(modelLocation, context, queryUser);
  }

  private static Model getModelFromFile(String modelLocation, DrillbitContext context, String queryUser) {
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
    if (plugin == null) {
      throw UserException.functionError()
        .message("Storage plugin '" + pluginInstanceName + "' was not found. Location: " + modelLocation)
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

    String fsUser = fileSystemUser(context, queryUser);
    byte[] modelBytes;
    try (DrillFileSystem dfs = openFileSystem(fileSystemPlugin.getFsConf(), fsUser);
         FSDataInputStream inputStream = dfs.open(new Path(fullFilePath))) {
      modelBytes = readFully(inputStream);
    } catch (IOException e) {
      throw UserException.functionError(e)
        .message("Unable to open model file " + fullFilePath + " (" + modelLocation + "): " + e.getMessage())
        .build(logger);
    }

    rejectDoctype(modelBytes, fullFilePath, modelLocation);

    try (InputStream clean = new ByteArrayInputStream(modelBytes)) {
      return Model.fromInputStream(clean);
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
   * The identity that opens the model file: the query user when impersonation is on, and
   * {@code null} (meaning the Drillbit process user) when it is off. Without this, a user
   * who may not select a file could still make the Drillbit read it through {@code predict()}.
   */
  static String fileSystemUser(DrillbitContext context, String queryUser) {
    boolean impersonation = context != null
      && context.getConfig().getBoolean(ExecConstants.IMPERSONATION_ENABLED);
    return fileSystemUser(impersonation, queryUser);
  }

  static String fileSystemUser(boolean impersonationEnabled, String queryUser) {
    if (impersonationEnabled && queryUser != null && !queryUser.trim().isEmpty()) {
      return queryUser;
    }
    return null;
  }

  /** Opens the file system as {@code fsUser}, or as the process user when that is null. */
  static DrillFileSystem openFileSystem(Configuration fsConf, String fsUser) throws IOException {
    if (fsUser != null) {
      return ImpersonationUtil.createFileSystem(fsUser, fsConf);
    }
    return new DrillFileSystem(fsConf);
  }

  private static byte[] readFully(InputStream in) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] chunk = new byte[8192];
    int read;
    while ((read = in.read(chunk)) >= 0) {
      out.write(chunk, 0, read);
    }
    return out.toByteArray();
  }

  /**
   * Refuses a model file that declares a DOCTYPE, before pmml4s ever sees it.
   *
   * <p>pmml4s parses PMML through its own {@link javax.xml.stream.XMLInputFactory} with default
   * settings and exposes no hook on it. On Drill's classpath that factory is Woodstox, which
   * resolves external entities and external DTD subsets, so the content of a model file could
   * otherwise read local files, probe for their existence, and make the Drillbit fetch a URL of
   * the file author's choosing. File uploads land arbitrary user bytes in a workspace by design,
   * so untrusted XML is the expected input here.
   *
   * <p>PMML has no legitimate use for a DTD, and every one of those attacks needs a DOCTYPE: an
   * internal subset to declare entities, an external subset to fetch one. Refusing the literal
   * closes all of them without depending on parser properties, which Woodstox honours by
   * silently ignoring the declaration rather than reporting it. NUL bytes are dropped first so
   * a UTF-16 encoded file cannot hide the declaration from the scan.
   */
  static void rejectDoctype(byte[] model, String fullFilePath, String modelLocation) {
    StringBuilder prolog = new StringBuilder();
    for (byte b : model) {
      if (b != 0) {
        prolog.append((char) (b & 0xFF));
      }
    }
    if (prolog.indexOf("<!DOCTYPE") >= 0) {
      throw UserException.functionError()
        .message("Unable to read model file " + fullFilePath + " (" + modelLocation
          + ") as a PMML model: it declares a DOCTYPE. A model file must not contain a DOCTYPE "
          + "declaration, because a DTD can read local files and make the server fetch URLs.")
        .build(logger);
    }
  }

  /**
   * Resolves a {@code plugin.workspace.path} or {@code plugin.path} string to a file system
   * path and confines it to the workspace. A name that resolves outside the workspace root
   * (through {@code ..} or an absolute path) is refused. Where no workspace location applies
   * (a plugin without workspaces, such as the classpath plugin, or a missing default
   * workspace) the name must be relative and must not climb out of its starting folder.
   */
  protected static String getFullFilePath(String location, FileSystemConfig config) {
    String clean = location.replace("`", "");
    String[] parts = clean.split("\\.", 3);
    String fileSystemName = parts[0];
    Map<String, WorkspaceConfig> workspaces = config.getWorkspaces();

    String workspaceLocation = null;
    String fileName;
    if (parts.length == 3 && workspaces.containsKey(parts[1])) {
      // plugin.workspace.path
      workspaceLocation = workspaces.get(parts[1]).getLocation();
      fileName = parts[2];
    } else {
      // plugin.path, where the path itself may contain dots
      fileName = clean.substring(Math.min(clean.length(), fileSystemName.length() + 1));
      WorkspaceConfig defaultWorkspace = workspaces.get("default");
      if (defaultWorkspace != null) {
        workspaceLocation = defaultWorkspace.getLocation();
      }
    }

    if (workspaceLocation == null || workspaceLocation.isEmpty()) {
      return confineRelative(fileName, location);
    }
    return resolveInside(new Path(workspaceLocation), fileName, location).toString();
  }

  private static String confineRelative(String fileName, String location) {
    if (fileName.startsWith("/") || safePath(fileName, location).isAbsolute()) {
      throw outsideWorkspace(location);
    }
    String normalized = safePath(fileName, location).toUri().normalize().getPath();
    if (normalized.isEmpty() || normalized.equals("..") || normalized.startsWith("../")) {
      throw outsideWorkspace(location);
    }
    return fileName;
  }

  private static Path safePath(String fileName, String location) {
    try {
      return new Path(fileName);
    } catch (IllegalArgumentException e) {
      throw outsideWorkspace(location);
    }
  }

  /**
   * The path of {@code name} inside {@code dir}. The prefix test is trailing-slash aware so a
   * workspace at the storage root works, and a name that resolves back onto the folder itself
   * is refused.
   */
  static Path resolveInside(Path dir, String name, String location) {
    Path target;
    try {
      target = new Path(dir, name);
    } catch (IllegalArgumentException e) {
      throw outsideWorkspace(location);
    }
    String dirUri = stripEmptyAuthority(dir.toUri().normalize().toString());
    String prefix = dirUri.endsWith("/") ? dirUri : dirUri + "/";
    String targetUri = stripEmptyAuthority(target.toUri().normalize().toString());
    // A workspace at the storage root leaves a climbing segment in place: /.. normalizes to
    // itself, so the prefix test alone would accept it.
    if (targetUri.endsWith("/..") || targetUri.contains("/../")) {
      throw outsideWorkspace(location);
    }
    if (!targetUri.startsWith(prefix) || targetUri.length() == prefix.length()) {
      throw outsideWorkspace(location);
    }
    return new Path(target.toUri().normalize());
  }

  /**
   * Drops an empty URI authority, so a workspace written as {@code file:///data} compares
   * against the {@code file:/data/...} form that resolving a name against it produces.
   */
  private static String stripEmptyAuthority(String uri) {
    return uri.replaceFirst("^([a-zA-Z][a-zA-Z0-9+.-]*):///", "$1:/");
  }

  private static UserException outsideWorkspace(String location) {
    return UserException.functionError()
      .message("The model location does not resolve to a file inside its workspace: " + location)
      .build(logger);
  }

  /**
   * Returns a prediction from a machine learning model. The model must be derived from a
   * PMML file and stored in a {@link Model} object. Returns a Map of probabilities and other
   * attributes of the prediction.
   *
   * @param model The trained model.
   * @param featureHolders An array of {@link FieldReader} containing observations. An unset
   *     (NULL) reader is passed to the model as a missing value.
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
      if (!currentReader.isSet()) {
        // A NULL column is a missing value, which pmml4s applies the model's own
        // missing-value handling to. Throwing here would fail a whole query over one gap.
        features[i] = null;
      } else if (dataType == MinorType.VARCHAR) {
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
