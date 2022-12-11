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

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxFile;
import com.box.sdk.BoxFolder;
import com.box.sdk.BoxFolder.Info;
import com.box.sdk.BoxItem;
import com.box.sdk.BoxSearch;
import com.box.sdk.BoxSearchParameters;
import com.box.sdk.PartialCollection;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.security.CredentialsProvider;
import org.apache.drill.exec.oauth.PersistentTokenTable;
import org.apache.drill.exec.store.security.oauth.OAuthTokenCredentials;
import org.apache.drill.exec.store.security.oauth.OAuthTokenCredentials.Builder;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class BoxFileSystem extends OAuthEnabledFileSystem {

  private static final Logger logger = LoggerFactory.getLogger(BoxFileSystem.class);
  private static final String TIMEOUT_DEFAULT = "5000";
  private static final List<String> SEARCH_CONTENT_TYPES = new ArrayList<>(Collections.singletonList("name"));
  private Path workingDirectory;
  private BoxAPIConnection client;
  private String workingDirectoryID;
  private BoxFolder rootFolder;
  private boolean usesDeveloperToken;
  private final List<String> ancestorFolderIDs = new ArrayList<>();
  private final Map<Path, BoxItem> itemCache = new HashMap<>();

  /**
   * Returns a URI which identifies this FileSystem.
   *
   * @return the URI of this filesystem.
   */
  @Override
  public URI getUri() {
    try {
      return new URI("box:///");
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Opens an FSDataInputStream at the indicated Path.
   *
   * @param inputPath the file name to open
   * @param bufferSize the size of the buffer to be used.
   * @throws IOException IO failure
   */
  @Override
  public FSDataInputStream open(Path inputPath, int bufferSize) throws IOException {
    client = getClient();
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    BoxItem item = getItem(inputPath);
    if (item instanceof BoxFile) {
      BoxFile file = (BoxFile) item;
      updateTokens();

      file.download(out);
      updateTokens();

      FSDataInputStream fsDataInputStream = new FSDataInputStream(new SeekableByteArrayInputStream(out.toByteArray()));
      out.close();
      return fsDataInputStream;
    } else {
      throw new IOException("Attempted to read " + inputPath + " which is not a file.  Only files can be read by Box.");
    }
  }

  /**
   * Create an FSDataOutputStream at the indicated Path with write-progress
   * reporting.
   *
   * @param f           the file name to open
   * @param permission  file permission
   * @param overwrite   if a file with this name already exists, then if true,
   *                    the file will be overwritten, and if false an error will be thrown.
   * @param bufferSize  the size of the buffer to be used.
   * @param replication required block replication for the file.
   * @param blockSize   block size
   * @param progress    the progress reporter
   * @throws IOException IO failure
   * @see #setPermission(Path, FsPermission)
   */
  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite,
    int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException("Box is read only.");
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException("Box does not support append.");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new UnsupportedOperationException("Box does not support rename.");
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return false;
  }

  /**
   * List the statuses of the files/directories in the given path if the path is
   * a directory.
   * <p>
   * Does not guarantee to return the List of files/directories status in a
   * sorted order.
   * <p>
   * Will not return null. Expect IOException upon access error.
   *
   * @param f given path
   * @return the statuses of the files/directories in the given patch
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException           see specific implementation
   */
  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    BoxItem incoming = getItem(f);

    // If the incoming item is a folder, find all the items in it,
    // and iterate over them.  This is not recursive, so it will only
    // iterate over the current folder.
    if (incoming instanceof BoxFolder) {
      BoxFolder folder = (BoxFolder) incoming;

      // Iterate over the children
      int itemCount = 0;
      FileStatus status;
      List<FileStatus> fileStatusList = new ArrayList<>();

      for (BoxItem.Info childInfo : folder.getChildren()) {
        Path newPath = new Path(f + childInfo.getName());
        if (childInfo instanceof BoxFolder.Info) {
          status = new FileStatus(childInfo.getSize(), true, 1,0,
            getModifiedMillis(childInfo), newPath);
          fileStatusList.add(status);
          itemCount++;
        } else if (childInfo instanceof BoxFile.Info) {
          status = new FileStatus(childInfo.getSize(), false, 1,0,
            getModifiedMillis(childInfo), newPath);
          fileStatusList.add(status);
          itemCount++;
        }
      }
      return fileStatusList.toArray(new FileStatus[itemCount]);
    } else if (incoming instanceof BoxFile) {
      // If the incoming object is a file, return the file info
      BoxFile infile = (BoxFile)incoming;

      FileStatus[] results = new FileStatus[1];
      results[0] = new FileStatus(infile.getInfo().getSize(), false, 1, 0,
        getModifiedMillis(infile), f);
      return results;
    }
    // Ignore web links
    return new FileStatus[0];
  }

  /**
   * Set the current working directory for the given FileSystem. All relative
   * paths will be resolved relative to it.
   *
   * @param new_dir Path of new working directory
   */
  @Override
  public void setWorkingDirectory(Path new_dir) {
    logger.debug("Setting working directory to: " + new_dir.getName());
    workingDirectory = new_dir;

    ancestorFolderIDs.clear();

    // Set the working directory id.
    if (StringUtils.isEmpty(workingDirectoryID) || new_dir.toString().contentEquals("/")) {
      workingDirectoryID = "0";
      ancestorFolderIDs.add("0");
    } else {
      // Split the path by the slash
      List<String> pathParts = new ArrayList<>(Arrays.asList(new_dir.toString().split("/")));
      for (String pathPart : pathParts) {
        BoxSearch search = new BoxSearch(client);
        updateTokens();

        BoxSearchParameters searchParams = new BoxSearchParameters();
        searchParams.setQuery(pathPart);
        searchParams.setContentTypes(SEARCH_CONTENT_TYPES);
        searchParams.setType("folder");
        searchParams.setAncestorFolderIds(ancestorFolderIDs);

        PartialCollection<BoxItem.Info> searchResults = search.searchRange(1, 3, searchParams);
        updateTokens();

        // Iterate over search results
        for (BoxItem.Info result : searchResults) {
          // Get the ID of the folder and add it to the ancestorFolderId list
          String id = result.getID();
          ancestorFolderIDs.add(id);
          break;
        }
      }
    }
  }

  @Override
  public Path getWorkingDirectory() {
    // If the working directory is empty, assume it is the root directory and set both
    // the id and working directory to root.
    if (StringUtils.isEmpty(workingDirectoryID)) {
      workingDirectory = new Path("/");
      workingDirectoryID = "0";
    }
    return workingDirectory;
  }


  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return false;
  }

  /**
   * Return a file status object that represents the path.
   *
   * @param f The path we want information from
   * @return a FileStatus object
   * @throws FileNotFoundException when the path does not exist
   * @throws IOException           see specific implementation
   */
  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    this.client = getClient();

    BoxItem pathItem = getItem(f);

    if (pathItem instanceof BoxFolder) {
      BoxFolder folder = (BoxFolder) pathItem;
      updateTokens();

      // Folders may have a modified date of zero.
      Info folderInfo = folder.getInfo();
      long size = folderInfo.getSize();

      return new FileStatus(size, true, 1,0, getModifiedMillis(pathItem), f);
    } else if (pathItem instanceof BoxFile) {
      BoxFile boxFile = (BoxFile) pathItem;
      BoxFile.Info fileInfo = boxFile.getInfo();
      long fileSize = fileInfo.getSize();

      return new FileStatus(fileSize, false, 1,0,
        getModifiedMillis(pathItem), f);
    } else {
      // The only other option here would be a BoxWebLink and Drill can't do anything with that.
      return new FileStatus();
    }
  }

  private BoxItem getItem(Path path) {
    if (itemCache.containsKey(path)) {
      return itemCache.get(path);
    }

    // Make sure the client is initialized
    this.client = getClient();

    // Check to see if it is the root
    if (path.isRoot()) {
      if (rootFolder == null) {
        rootFolder = BoxFolder.getRootFolder(client);
        updateTokens();
        itemCache.put(path, rootFolder);
      }
      return rootFolder;
    }

    // Next check to see if the item ends in a file extension.
    // In Box, you can only access items via their ID, so rather than making numerous API
    // calls to traverse a directory path, we will use Box's search API to find candidates.
    // If there is one candidate, return that.  If not, we will use the item's parents to make
    // sure it is the correct item.
    long offsetValue = 0;
    long limitValue = 100;

    BoxSearch search = new BoxSearch(client);
    updateTokens();
    BoxSearchParameters searchParams = new BoxSearchParameters();
    searchParams.setQuery(path.getName());
    searchParams.setContentTypes(SEARCH_CONTENT_TYPES);

    // Get the file extension and use it as a search parameter
    String fileExtension = FilenameUtils.getExtension(path.getName());

    // If there is no extension, assume the path is a folder
    if (StringUtils.isEmpty(fileExtension)) {
      searchParams.setType("folder");
    } else {
      searchParams.setType("file");
      searchParams.setFileExtensions(Collections.singletonList(fileExtension));
    }

    if (ancestorFolderIDs.size() > 0) {
      searchParams.setAncestorFolderIds(ancestorFolderIDs);
    }

    PartialCollection<BoxItem.Info> searchResults =
      search.searchRange(offsetValue, limitValue, searchParams);
    updateTokens();

    // Assuming that the first result is the one we want, return the item for that.
    for (BoxItem.Info resultInfo : searchResults) {
      String id = resultInfo.getID();

      if (resultInfo.getType().contentEquals("file")) {
        BoxFile file = new BoxFile(client, id);
        updateTokens();
        itemCache.put(path, file);
        return file;
      } else if (resultInfo.getType().contentEquals("folder")) {
        BoxFolder folder = new BoxFolder(client, id);
        updateTokens();
        itemCache.put(path, folder);
        return folder;
      }
    }

    return null;
  }

  /**
   * Updates the OAuth tokens.  Box API tokens seem to be very short-lived and change after every
   * API call. Be sure to call this method after any call that actually calls the Box API.
   * <p>
   * If the Box API client uses a developer token, the function will simply return.
   */
  private void updateTokens() {
    if (client == null || usesDeveloperToken) {
      return;
    } else if (client.canRefresh() && client.needsRefresh()) {
      if (!client.canRefresh()) {
        throw UserException.connectionError()
          .message("Box file system missing refresh token. Please reauthenticate to obtain a refresh token.")
          .build(logger);
      }
      super.updateTokens(client.getAccessToken(), client.getRefreshToken(), String.valueOf(client.getExpires()));
    }
  }


  /**
   * The Box client can use either OAuth 2.0 or a static developer token. The OAuth tokens are very
   * short lived and hence are difficult to use for testing.
   * @return An authenticated {@link BoxAPIConnection} Box client
   */
  private BoxAPIConnection getClient() {
    if (this.client != null) {
      return client;
    }

    // Get timeout values from configuration.
    int connectionTimeout = Integer.parseInt(this.getConf().get("boxConnectionTimeout", TIMEOUT_DEFAULT));
    int readTimeout = Integer.parseInt(this.getConf().get("boxReadTimeout", TIMEOUT_DEFAULT));

    // If the developer token is populated, use this rather than the OAuth tokens to create the client
    // This should only be used for testing.
    String developerToken = this.getConf().get("boxAccessToken", "");
    if (StringUtils.isNotEmpty(developerToken)) {
      BoxAPIConnection client = new BoxAPIConnection(developerToken);
      client.setConnectTimeout(connectionTimeout);
      client.setReadTimeout(readTimeout);
      this.usesDeveloperToken = true;
      return client;
    }

    CredentialsProvider credentialsProvider = getCredentialsProvider();
    PersistentTokenTable tokenTable = getTokenTable();
    OAuthTokenCredentials credentials = new Builder()
      .setCredentialsProvider(credentialsProvider)
        .setTokenTable(tokenTable)
        .build()
        .get();

    BoxAPIConnection newClient = new BoxAPIConnection(credentials.getClientID(), credentials.getClientSecret(),
      tokenTable.getAccessToken(), tokenTable.getRefreshToken());
    newClient.setConnectTimeout(connectionTimeout);
    newClient.setReadTimeout(readTimeout);
    return newClient;
  }

  /**
   * Returns the milliseconds of the modified time. In some cases, this can be null, so these functions
   * avoid having multiple null checks all over the place.
   * @param item {@link BoxItem} of the input
   * @return The milliseconds of the modified time.
   */
  private long getModifiedMillis(BoxItem item) {
    return getModifiedMillis(item.getInfo());
  }

  /**
   * Returns the milliseconds of the modified time. In some cases, this can be null, so these functions
   * avoid having multiple null checks all over the place.
   * @param info {@link BoxItem.Info} of the input
   * @return The milliseconds of the modified time.
   */
  private long getModifiedMillis(BoxItem.Info info ) {
    Date modifiedDate = info.getModifiedAt();
    if (modifiedDate == null) {
      return 0;
    } else {
      return modifiedDate.getTime();
    }
  }
}
