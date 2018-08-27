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

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.base.Strings;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.util.DrillFileSystemUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Jackson serializable description of a file selection.
 */
public class FileSelection {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSelection.class);
  private static final String WILD_CARD = "*";

  private List<FileStatus> statuses;

  public List<String> files;
  /**
   * root path for the selections
   */
  public final String selectionRoot;
  /**
   * root path for the metadata cache file (if any)
   */
  public final String cacheFileRoot;

  /**
   * metadata context useful for metadata operations (if any)
   */
  private MetadataContext metaContext = null;

  /**
   * Indicates whether this selectionRoot is an empty directory
   */
  private boolean emptyDirectory;

  private enum StatusType {
    NOT_CHECKED,         // initial state
    NO_DIRS,             // no directories in this selection
    HAS_DIRS,            // directories were found in the selection
    EXPANDED_FULLY,      // whether selection fully expanded to files
    EXPANDED_PARTIAL     // whether selection partially expanded to only directories (not files)
  }

  private StatusType dirStatus;
  // whether this selection previously had a wildcard
  private boolean hadWildcard = false;
  // whether all partitions were previously pruned for this selection
  private boolean wasAllPartitionsPruned = false;

  /**
   * Creates a {@link FileSelection selection} out of given file statuses/files and selection root.
   *
   * @param statuses  list of file statuses
   * @param files  list of files
   * @param selectionRoot  root path for selections
   */
  public FileSelection(final List<FileStatus> statuses, final List<String> files, final String selectionRoot) {
    this(statuses, files, selectionRoot, null, false, StatusType.NOT_CHECKED);
  }

  public FileSelection(final List<FileStatus> statuses, final List<String> files, final String selectionRoot,
      final String cacheFileRoot, final boolean wasAllPartitionsPruned) {
    this(statuses, files, selectionRoot, cacheFileRoot, wasAllPartitionsPruned, StatusType.NOT_CHECKED);
  }

  public FileSelection(final List<FileStatus> statuses, final List<String> files, final String selectionRoot,
      final String cacheFileRoot, final boolean wasAllPartitionsPruned, final StatusType dirStatus) {
    this.statuses = statuses;
    this.files = files;
    this.selectionRoot = selectionRoot;
    this.dirStatus = dirStatus;
    this.cacheFileRoot = cacheFileRoot;
    this.wasAllPartitionsPruned = wasAllPartitionsPruned;
  }

  /**
   * Copy constructor for convenience.
   */
  protected FileSelection(final FileSelection selection) {
    Preconditions.checkNotNull(selection, "selection cannot be null");
    this.statuses = selection.statuses;
    this.files = selection.files;
    this.selectionRoot = selection.selectionRoot;
    this.dirStatus = selection.dirStatus;
    this.cacheFileRoot = selection.cacheFileRoot;
    this.metaContext = selection.metaContext;
    this.hadWildcard = selection.hadWildcard;
    this.wasAllPartitionsPruned = selection.wasAllPartitionsPruned;
  }

  public String getSelectionRoot() {
    return selectionRoot;
  }

  public List<FileStatus> getStatuses(final DrillFileSystem fs) throws IOException {
    Stopwatch timer = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;

    if (statuses == null)  {
      final List<FileStatus> newStatuses = Lists.newArrayList();
      for (final String pathStr:files) {
        newStatuses.add(fs.getFileStatus(new Path(pathStr)));
      }
      statuses = newStatuses;
    }
    if (timer != null) {
      logger.debug("FileSelection.getStatuses() took {} ms, numFiles: {}",
          timer.elapsed(TimeUnit.MILLISECONDS), statuses == null ? 0 : statuses.size());
      timer.stop();
    }

    return statuses;
  }

  public List<String> getFiles() {
    if (files == null) {
      final List<String> newFiles = Lists.newArrayList();
      for (final FileStatus status:statuses) {
        newFiles.add(status.getPath().toString());
      }
      files = newFiles;
    }
    return files;
  }

  public boolean containsDirectories(DrillFileSystem fs) throws IOException {
    if (dirStatus == StatusType.NOT_CHECKED) {
      dirStatus = StatusType.NO_DIRS;
      for (final FileStatus status : getStatuses(fs)) {
        if (status.isDirectory()) {
          dirStatus = StatusType.HAS_DIRS;
          break;
        }
      }
    }
    return dirStatus == StatusType.HAS_DIRS;
  }

  public FileSelection minusDirectories(DrillFileSystem fs) throws IOException {
    if (isExpandedFully()) {
      return this;
    }
    Stopwatch timer = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;
    List<FileStatus> statuses = getStatuses(fs);

    List<FileStatus> nonDirectories = Lists.newArrayList();
    for (FileStatus status : statuses) {
      nonDirectories.addAll(DrillFileSystemUtil.listFiles(fs, status.getPath(), true));
    }

    final FileSelection fileSel = create(nonDirectories, null, selectionRoot);
    if (timer != null) {
      logger.debug("FileSelection.minusDirectories() took {} ms, numFiles: {}", timer.elapsed(TimeUnit.MILLISECONDS), statuses.size());
      timer.stop();
    }

    // fileSel will be null if we query an empty folder
    if (fileSel != null) {
      fileSel.setExpandedFully();
    }

    return fileSel;
  }

  public FileStatus getFirstPath(DrillFileSystem fs) throws IOException {
    return getStatuses(fs).get(0);
  }

  public void setExpandedFully() {
    this.dirStatus = StatusType.EXPANDED_FULLY;
  }

  public boolean isExpandedFully() {
    return dirStatus == StatusType.EXPANDED_FULLY;
  }

  public void setExpandedPartial() {
    this.dirStatus = StatusType.EXPANDED_PARTIAL;
  }

  public boolean isExpandedPartial() {
    return dirStatus == StatusType.EXPANDED_PARTIAL;
  }

  public StatusType getDirStatus() {
    return dirStatus;
  }

  public boolean wasAllPartitionsPruned() {
    return this.wasAllPartitionsPruned;
  }

  /**
   * Returns longest common path for the given list of files.
   *
   * @param files  list of files.
   * @return  longest common path
   */
  private static String commonPathForFiles(final List<String> files) {
    if (files == null || files.isEmpty()) {
      return "";
    }

    final int total = files.size();
    final String[][] folders = new String[total][];
    int shortest = Integer.MAX_VALUE;
    for (int i = 0; i < total; i++) {
      final Path path = new Path(files.get(i));
      folders[i] = Path.getPathWithoutSchemeAndAuthority(path).toString().split(Path.SEPARATOR);
      shortest = Math.min(shortest, folders[i].length);
    }

    int latest;
    out:
    for (latest = 0; latest < shortest; latest++) {
      final String current = folders[0][latest];
      for (int i = 1; i < folders.length; i++) {
        if (!current.equals(folders[i][latest])) {
          break out;
        }
      }
    }
    final Path path = new Path(files.get(0));
    final URI uri = path.toUri();
    final String pathString = buildPath(folders[0], latest);
    return new Path(uri.getScheme(), uri.getAuthority(), pathString).toString();
  }

  private static String buildPath(final String[] path, final int folderIndex) {
    final StringBuilder builder = new StringBuilder();
    for (int i=0; i<folderIndex; i++) {
      builder.append(path[i]).append(Path.SEPARATOR);
    }
    builder.deleteCharAt(builder.length()-1);
    return builder.toString();
  }

  public static FileSelection create(final DrillFileSystem fs, final String parent, final String path,
      final boolean allowAccessOutsideWorkspace) throws IOException {
    Stopwatch timer = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;
    boolean hasWildcard = path.contains(WILD_CARD);

    final Path combined = new Path(parent, removeLeadingSlash(path));
    if (!allowAccessOutsideWorkspace) {
      checkBackPaths(new Path(parent).toUri().getPath(), combined.toUri().getPath(), path);
    }
    final FileStatus[] statuses = fs.globStatus(combined); // note: this would expand wildcards
    if (statuses == null) {
      return null;
    }
    final FileSelection fileSel = create(Lists.newArrayList(statuses), null, combined.toUri().getPath());
    if (timer != null) {
      logger.debug("FileSelection.create() took {} ms ", timer.elapsed(TimeUnit.MILLISECONDS));
      timer.stop();
    }
    if (fileSel == null) {
      return null;
    }
    fileSel.setHadWildcard(hasWildcard);
    return fileSel;

  }

  /**
   * Creates a {@link FileSelection selection} with the given file statuses/files and selection root.
   *
   * @param statuses  list of file statuses
   * @param files  list of files
   * @param root  root path for selections
   * @param cacheFileRoot root path for metadata cache (null for no metadata cache)
   * @return  null if creation of {@link FileSelection} fails with an {@link IllegalArgumentException}
   *          otherwise a new selection.
   *
   * @see FileSelection#FileSelection(List, List, String)
   */
  public static FileSelection create(final List<FileStatus> statuses, final List<String> files, final String root,
      final String cacheFileRoot, final boolean wasAllPartitionsPruned) {
    final boolean bothNonEmptySelection = (statuses != null && statuses.size() > 0) && (files != null && files.size() > 0);
    final boolean bothEmptySelection = (statuses == null || statuses.size() == 0) && (files == null || files.size() == 0);

    if (bothNonEmptySelection || bothEmptySelection) {
      return null;
    }

    final String selectionRoot;
    if (statuses == null || statuses.isEmpty()) {
      selectionRoot = commonPathForFiles(files);
    } else {
      if (Strings.isNullOrEmpty(root)) {
        throw new DrillRuntimeException("Selection root is null or empty" + root);
      }
      final Path rootPath = handleWildCard(root);
      final URI uri = statuses.get(0).getPath().toUri();
      final Path path = new Path(uri.getScheme(), uri.getAuthority(), rootPath.toUri().getPath());
      selectionRoot = path.toString();
    }
    return new FileSelection(statuses, files, selectionRoot, cacheFileRoot, wasAllPartitionsPruned);
  }

  public static FileSelection create(final List<FileStatus> statuses, final List<String> files, final String root) {
    return FileSelection.create(statuses, files, root, null, false);
  }

  public static FileSelection createFromDirectories(final List<String> dirPaths, final FileSelection selection,
      final String cacheFileRoot) {
    Stopwatch timer = logger.isDebugEnabled() ? Stopwatch.createStarted() : null;
    final String root = selection.getSelectionRoot();
    if (Strings.isNullOrEmpty(root)) {
      throw new DrillRuntimeException("Selection root is null or empty" + root);
    }
    if (dirPaths == null || dirPaths.isEmpty()) {
      throw new DrillRuntimeException("List of directories is null or empty");
    }

    List<String> dirs = Lists.newArrayList();

    if (selection.hadWildcard()) { // for wildcard the directory list should have already been expanded
      for (FileStatus status : selection.getFileStatuses()) {
        dirs.add(status.getPath().toString());
      }
    } else {
      dirs.addAll(dirPaths);
    }

    final Path rootPath = handleWildCard(root);
    // final URI uri = dirPaths.get(0).toUri();
    final URI uri = selection.getFileStatuses().get(0).getPath().toUri();
    final Path path = new Path(uri.getScheme(), uri.getAuthority(), rootPath.toUri().getPath());
    FileSelection fileSel = new FileSelection(null, dirs, path.toString(), cacheFileRoot, false);
    fileSel.setHadWildcard(selection.hadWildcard());
    if (timer != null) {
      logger.debug("FileSelection.createFromDirectories() took {} ms ", timer.elapsed(TimeUnit.MILLISECONDS));
      timer.stop();
    }
    return fileSel;
  }

  private static Path handleWildCard(final String root) {
    if (root.contains(WILD_CARD)) {
      int idx = root.indexOf(WILD_CARD); // first wild card in the path
      idx = root.lastIndexOf('/', idx); // file separator right before the first wild card
      final String newRoot = root.substring(0, idx);
      if (newRoot.length() == 0) {
          // Ensure that we always return a valid root.
          return new Path("/");
      }
      return new Path(newRoot);
    } else {
      return new Path(root);
    }
  }

  public static String removeLeadingSlash(String path) {
    if (!path.isEmpty() && path.charAt(0) == '/') {
      String newPath = path.substring(1);
      return removeLeadingSlash(newPath);
    } else {
      return path;
    }
  }

  /**
   * Check if the path is a valid sub path under the parent after removing backpaths. Throw an exception if
   * it is not. We pass subpath in as a parameter only for the error message
   *
   * @param parent The parent path (the workspace directory).
   * @param combinedPath The workspace directory and (relative) subpath path combined.
   * @param subpath For error message only, the subpath
   */
  public static void checkBackPaths(String parent, String combinedPath, String subpath) {
    Preconditions.checkArgument(!parent.isEmpty(), "Invalid root (" + parent + ") in file selection path.");
    Preconditions.checkArgument(!combinedPath.isEmpty(), "Empty path (" + combinedPath + "( in file selection path.");

    if (!combinedPath.startsWith(parent)) {
      StringBuilder msg = new StringBuilder();
      msg.append("Invalid path : ").append(subpath).append(" takes you outside the workspace.");
      throw new IllegalArgumentException(msg.toString());
    }
  }

  public List<FileStatus> getFileStatuses() {
    return statuses;
  }

  public boolean supportDirPrunig() {
    if (isExpandedFully() || isExpandedPartial()) {
      if (!wasAllPartitionsPruned) {
        return true;
      }
    }
    return false;
  }

  public void setHadWildcard(boolean wc) {
    this.hadWildcard = wc;
  }

  public boolean hadWildcard() {
    return this.hadWildcard;
  }

  public String getCacheFileRoot() {
    return cacheFileRoot;
  }

  public void setMetaContext(MetadataContext context) {
    metaContext = context;
  }

  public MetadataContext getMetaContext() {
    return metaContext;
  }

  /**
   * @return true if this {@link FileSelection#selectionRoot} points to an empty directory, false otherwise
   */
  public boolean isEmptyDirectory() {
    return emptyDirectory;
  }

  /**
   * Setting {@link FileSelection#emptyDirectory} as true allows to identify this {@link FileSelection#selectionRoot}
   * as an empty directory
   */
  public void setEmptyDirectoryStatus() {
    this.emptyDirectory = true;
  }


  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("root=").append(this.selectionRoot);

    sb.append("files=[");
    boolean isFirst = true;
    for (final String file : this.files) {
      if (isFirst) {
        isFirst = false;
        sb.append(file);
      } else {
        sb.append(",");
        sb.append(file);
      }
    }
    sb.append("]");

    return sb.toString();
  }

}
