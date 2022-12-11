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

package org.apache.drill.exec.store.dfs;

import com.dropbox.core.DbxDownloader;
import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.oauth.DbxCredential;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.FolderMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.oauth.PersistentTokenTable;
import org.apache.drill.exec.store.security.oauth.OAuthTokenCredentials;
import org.apache.drill.exec.vector.complex.fn.SeekableBAIS;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DropboxFileSystem extends OAuthEnabledFileSystem {
  private static final Logger logger = LoggerFactory.getLogger(DropboxFileSystem.class);

  private static final String ERROR_MSG = "Dropbox is read only.";
  private static final String APP_IDENTIFIER = "Apache/Drill";
  private Path workingDirectory;
  private DbxClientV2 client;
  private DbxCredential dbxCredential;
  private DbxRequestConfig config;
  private FileStatus[] fileStatuses;
  private final Map<String,FileStatus> fileStatusCache = new HashMap<>();
  private boolean usesDeveloperToken;

  @Override
  public URI getUri() {
    try {
      return new URI("dropbox:///");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    FSDataInputStream fsDataInputStream;
    String filename = getFileName(path);
    client = getClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (DbxDownloader<FileMetadata> downloader = client.files().download(filename)) {
      downloader.download(out);
      updateTokens();
      fsDataInputStream = new FSDataInputStream(new SeekableBAIS(out.toByteArray()));
    } catch (DbxException e) {
      throw new IOException(e.getMessage());
    }
    return fsDataInputStream;
  }

  @Override
  public FSDataOutputStream create(Path f,
                                   FsPermission permission,
                                   boolean overwrite,
                                   int bufferSize,
                                   short replication,
                                   long blockSize,
                                   Progressable progress) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return false;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    client = getClient();
    List<FileStatus> fileStatusList = new ArrayList<>();

    // Get files and folder metadata from Dropbox root directory
    try {
      String pathString;
      if (path.isRoot()) {
        pathString = "";
      } else {
        pathString = path.toString().replace("dropbox:", "");
      }

      ListFolderResult result = client.files().listFolder(pathString);
      updateTokens();
      while (true) {
        for (Metadata metadata : result.getEntries()) {
          fileStatusList.add(getFileInformation(metadata));
        }
        if (!result.getHasMore()) {
          break;
        }
        result = client.files().listFolderContinue(result.getCursor());
        updateTokens();
      }
    } catch (DbxException e) {
      throw new IOException(e.getMessage());
    }

    // Convert to Array
    fileStatuses = new FileStatus[fileStatusList.size()];
    for (int i = 0; i < fileStatusList.size(); i++) {
      fileStatuses[i] = fileStatusList.get(i);
    }

    return fileStatuses;
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    logger.debug("Setting working directory to: " + new_dir.getName());
    workingDirectory = new_dir;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDirectory;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    throw new IOException(ERROR_MSG);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    String filePath  = Path.getPathWithoutSchemeAndAuthority(path).toString();
    /*
     * Dropbox does not allow metadata calls on the root directory
     */
    if (filePath.equalsIgnoreCase("/")) {
      return new FileStatus(0, true, 1, 0, 0, new Path("/"));
    }
    client = getClient();
    try {
      Metadata metadata = client.files().getMetadata(filePath);
      updateTokens();
      return getFileInformation(metadata);
    } catch (Exception e) {
      throw new IOException("Error accessing file " + filePath + "\n" + e.getMessage());
    }
  }

  private FileStatus getFileInformation(Metadata metadata) {
    if (fileStatusCache.containsKey(metadata.getPathLower())){
      return fileStatusCache.get(metadata.getPathLower());
    }

    FileStatus result;
    if (isDirectory(metadata)) {
      // Note:  At the time of implementation, DropBox does not provide an efficient way of
      // getting the size and/or modification times for folders.
      result = new FileStatus(0, true, 1, 0, 0, new Path(metadata.getPathLower()));
    } else {
      FileMetadata fileMetadata = (FileMetadata) metadata;
      result = new FileStatus(fileMetadata.getSize(), false, 1, 0, fileMetadata.getClientModified().getTime(), new Path(metadata.getPathLower()));
    }

    fileStatusCache.put(metadata.getPathLower(), result);
    return result;
  }

  private DbxClientV2 getClient() {
    if (this.client != null) {
      return client;
    }

    // read preferred client identifier from config or use "Apache/Drill"
    String clientIdentifier = this.getConf().get("clientIdentifier", APP_IDENTIFIER);
    logger.info("Creating dropbox client with client identifier: {}", clientIdentifier);

    config = DbxRequestConfig.newBuilder(clientIdentifier)
      .withAutoRetryEnabled(5)
      .build();

    // read access token from config or credentials provider
    logger.info("Reading dropbox access token from configuration or credentials provider");
    String accessToken = this.getConf().get("dropboxAccessToken", "");

    // If the user is using a static developer token, return a client with that.
    if (StringUtils.isNotEmpty(accessToken)) {
      client = new DbxClientV2(config, accessToken);
      usesDeveloperToken = true;
    } else {
      // Otherwise, use OAuth tokens
      CredentialsProvider credentialsProvider = getCredentialsProvider();
      PersistentTokenTable tokenTable = getTokenTable();
      OAuthTokenCredentials credentials = new OAuthTokenCredentials.Builder()
        .setCredentialsProvider(credentialsProvider)
        .setTokenTable(tokenTable)
        .build()
        .get();

      long expiresIn = 0;
      if (StringUtils.isNotEmpty(credentials.getExpiresIn())) {
        expiresIn = Long.parseLong(credentials.getExpiresIn());
      }

      dbxCredential = new DbxCredential(credentials.getAccessToken(), expiresIn, credentials.getRefreshToken(),
        credentials.getClientID(), credentials.getClientSecret());

      client = new DbxClientV2(config, dbxCredential);
      usesDeveloperToken = false;
    }

    return client;
  }

  private void updateTokens() {
    if (client == null || usesDeveloperToken) {
      return;
    } else if (dbxCredential.aboutToExpire()) {
      try {
        dbxCredential.refresh(config);
      } catch (DbxException e) {
        throw UserException.connectionError(e)
          .message("Error refreshing Dropbox OAuth tokens: " + e.getMessage())
          .build(logger);
      }
      // Update the tokens in Drill
      updateTokens(dbxCredential.getAccessToken(), dbxCredential.getRefreshToken(), String.valueOf(dbxCredential.getExpiresAt()));
    }
  }

  private boolean isDirectory(Metadata metadata) {
    return metadata instanceof FolderMetadata;
  }

  private String getFileName(Path path){
    return path.toUri().getPath();
  }
}
