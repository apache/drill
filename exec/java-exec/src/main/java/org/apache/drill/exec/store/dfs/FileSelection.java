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
package org.apache.drill.exec.store.dfs;

import java.io.IOException;
import java.net.URI;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Jackson serializable description of a file selection.
 */
public class FileSelection {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSelection.class);
  private static final String PATH_SEPARATOR = System.getProperty("file.separator");
  private static final String WILD_CARD = "*";

  private List<FileStatus> statuses;

  public List<String> files;
  public final String selectionRoot;

  private enum StatusType {
    NOT_CHECKED,         // initial state
    NO_DIRS,             // no directories in this selection
    HAS_DIRS,            // directories were found in the selection
    EXPANDED             // whether this selection has been expanded to files
  }

  private StatusType dirStatus;

  /**
   * Creates a {@link FileSelection selection} out of given file statuses/files and selection root.
   *
   * @param statuses  list of file statuses
   * @param files  list of files
   * @param selectionRoot  root path for selections
   */
  public FileSelection(final List<FileStatus> statuses, final List<String> files, final String selectionRoot) {
    this.statuses = statuses;
    this.files = files;
    this.selectionRoot = Preconditions.checkNotNull(selectionRoot);
    this.dirStatus = StatusType.NOT_CHECKED;
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
  }

  public String getSelectionRoot() {
    return selectionRoot;
  }

  public List<FileStatus> getStatuses(final DrillFileSystem fs) throws IOException {
    Stopwatch timer = Stopwatch.createStarted();

    if (statuses == null)  {
      final List<FileStatus> newStatuses = Lists.newArrayList();
      for (final String pathStr:files) {
        newStatuses.add(fs.getFileStatus(new Path(pathStr)));
      }
      statuses = newStatuses;
    }
    logger.debug("FileSelection.getStatuses() took {} ms, numFiles: {}",
        timer.elapsed(TimeUnit.MILLISECONDS), statuses == null ? 0 : statuses.size());

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
    if (isExpanded()) {
      return this;
    }
    Stopwatch timer = Stopwatch.createStarted();
    final List<FileStatus> statuses = getStatuses(fs);
    final int total = statuses.size();
    final Path[] paths = new Path[total];
    for (int i=0; i<total; i++) {
      paths[i] = statuses.get(i).getPath();
    }
    final List<FileStatus> allStats = fs.list(true, paths);
    final List<FileStatus> nonDirectories = Lists.newArrayList(Iterables.filter(allStats, new Predicate<FileStatus>() {
      @Override
      public boolean apply(@Nullable FileStatus status) {
        return !status.isDirectory();
      }
    }));

    final FileSelection fileSel = create(nonDirectories, null, selectionRoot);
    logger.debug("FileSelection.minusDirectories() took {} ms, numFiles: {}",
        timer.elapsed(TimeUnit.MILLISECONDS), total);

    fileSel.setExpanded();
    return fileSel;
  }

  public FileStatus getFirstPath(DrillFileSystem fs) throws IOException {
    return getStatuses(fs).get(0);
  }

  public void setExpanded() {
    this.dirStatus = StatusType.EXPANDED;
  }

  public boolean isExpanded() {
    return dirStatus == StatusType.EXPANDED;
  }

  private static String commonPath(final List<FileStatus> statuses) {
    if (statuses == null || statuses.isEmpty()) {
      return "";
    }

    final List<String> files = Lists.newArrayList();
    for (final FileStatus status : statuses) {
      files.add(status.getPath().toString());
    }
    return commonPathForFiles(files);
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
      folders[i] = Path.getPathWithoutSchemeAndAuthority(path).toString().split(PATH_SEPARATOR);
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
      builder.append(path[i]).append(PATH_SEPARATOR);
    }
    builder.deleteCharAt(builder.length()-1);
    return builder.toString();
  }

  public static FileSelection create(final DrillFileSystem fs, final String parent, final String path) throws IOException {
    Stopwatch timer = Stopwatch.createStarted();
    final Path combined = new Path(parent, removeLeadingSlash(path));
    final FileStatus[] statuses = fs.globStatus(combined);
    if (statuses == null) {
      return null;
    }
    final FileSelection fileSel = create(Lists.newArrayList(statuses), null, combined.toUri().toString());
    logger.debug("FileSelection.create() took {} ms ", timer.elapsed(TimeUnit.MILLISECONDS));
    return fileSel;

  }

  /**
   * Creates a {@link FileSelection selection} with the given file statuses/files and selection root.
   *
   * @param statuses  list of file statuses
   * @param files  list of files
   * @param root  root path for selections
   *
   * @return  null if creation of {@link FileSelection} fails with an {@link IllegalArgumentException}
   *          otherwise a new selection.
   *
   * @see FileSelection#FileSelection(List, List, String)
   */
  public static FileSelection create(final List<FileStatus> statuses, final List<String> files, final String root) {
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
    return new FileSelection(statuses, files, selectionRoot);
  }

  private static Path handleWildCard(final String root) {
    if (root.contains(WILD_CARD)) {
      int idx = root.indexOf(WILD_CARD); // first wild card in the path
      idx = root.lastIndexOf(PATH_SEPARATOR, idx); // file separator right before the first wild card
      final String newRoot = root.substring(0, idx);
      return new Path(newRoot);
    } else {
      return new Path(root);
    }
  }

  private static String removeLeadingSlash(String path) {
    if (path.charAt(0) == '/') {
      String newPath = path.substring(1);
      return removeLeadingSlash(newPath);
    } else {
      return path;
    }
  }

  public List<FileStatus> getFileStatuses() {
    return statuses;
  }

  public boolean supportDirPrunig() {
    return isExpanded(); // currently we only support pruning if the directories have been expanded (this may change in the future)
  }

}
