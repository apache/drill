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
package org.apache.drill.exec.util;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Helper class that provides methods to list directories or file or both statuses.
 * Can list statuses recursive and apply custom filters.
 */
public class FileSystemUtil {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemUtil.class);

  /**
   * Filter that will accept all files and directories.
   */
  public static final PathFilter DUMMY_FILTER = path -> true;

  /**
   * Returns statuses of all directories present in given path applying custom filters if present.
   * Will also include nested directories if recursive flag is set to true.
   *
   * @param fs current file system
   * @param path path to directory
   * @param recursive true if nested directories should be included
   * @param filters list of custom filters (optional)
   * @return list of matching directory statuses
   */
  public static List<FileStatus> listDirectories(final FileSystem fs, Path path, boolean recursive, PathFilter... filters) throws IOException {
    List<FileStatus> statuses = new ArrayList<>();
    listDirectories(fs, path, recursive, false, statuses, mergeFilters(filters));
    return statuses;
  }

  /**
   * Returns statuses of all directories present in given path applying custom filters if present.
   * Will also include nested directories if recursive flag is set to true.
   * Will ignore all exceptions during listing if any.
   *
   * @param fs current file system
   * @param path path to directory
   * @param recursive true if nested directories should be included
   * @param filters list of custom filters (optional)
   * @return list of matching directory statuses
   */
  public static List<FileStatus> listDirectoriesSafe(final FileSystem fs, Path path, boolean recursive, PathFilter... filters) {
    List<FileStatus> statuses = new ArrayList<>();
    try {
      listDirectories(fs, path, recursive, true, statuses, mergeFilters(filters));
    } catch (Exception e) {
      // all exceptions are ignored
    }
    return statuses;
  }

  /**
   * Returns statuses of all files present in given path applying custom filters if present.
   * Will also include files from nested directories if recursive flag is set to true.
   *
   * @param fs current file system
   * @param path path to file or directory
   * @param recursive true if files in nested directories should be included
   * @param filters list of custom filters (optional)
   * @return list of matching file statuses
   */
  public static List<FileStatus> listFiles(FileSystem fs, Path path, boolean recursive, PathFilter... filters) throws IOException {
    List<FileStatus> statuses = new ArrayList<>();
    listFiles(fs, path, recursive, false, statuses, mergeFilters(filters));
    return statuses;
  }

  /**
   * Returns statuses of all files present in given path applying custom filters if present.
   * Will also include files from nested directories if recursive flag is set to true.
   *
   * @param fs current file system
   * @param path path to file or directory
   * @param recursive true if files in nested directories should be included
   * @param filters list of custom filters (optional)
   * @return list of matching file statuses
   */
  public static List<FileStatus> listFilesSafe(FileSystem fs, Path path, boolean recursive, PathFilter... filters) {
    List<FileStatus> statuses = new ArrayList<>();
    try {
      listFiles(fs, path, recursive, true, statuses, mergeFilters(filters));
    } catch (Exception e) {
      // all exceptions are ignored
    }
    return statuses;
  }

  /**
   * Returns statuses of all directories and files present in given path applying custom filters if present.
   * Will also include nested directories and their files if recursive flag is set to true.
   *
   * @param fs current file system
   * @param path path to file or directory
   * @param recursive true if nested directories and their files should be included
   * @param filters list of custom filters (optional)
   * @return list of matching directory and file statuses
   */
  public static List<FileStatus> listAll(FileSystem fs, Path path, boolean recursive, PathFilter... filters) throws IOException {
    List<FileStatus> statuses = new ArrayList<>();
    listAll(fs, path, recursive, false, statuses, mergeFilters(filters));
    return statuses;
  }

  /**
   * Returns statuses of all directories and files present in given path applying custom filters if present.
   * Will also include nested directories and their files if recursive flag is set to true.
   * Will ignore all exceptions during listing if any.
   *
   * @param fs current file system
   * @param path path to file or directory
   * @param recursive true if nested directories and their files should be included
   * @param filters list of custom filters (optional)
   * @return list of matching directory and file statuses
   */
  public static List<FileStatus> listAllSafe(FileSystem fs, Path path, boolean recursive, PathFilter... filters) {
    List<FileStatus> statuses = new ArrayList<>();
    try {
      listAll(fs, path, recursive, true, statuses, mergeFilters(filters));
    } catch (Exception e) {
      // all exceptions are ignored
    }
    return statuses;
  }

  /**
   * Merges given filter with array of filters.
   * If array of filters is null or empty, will return given filter.
   *
   * @param filter given filter
   * @param filters array of filters
   * @return one filter that combines all given filters
   */
  public static PathFilter mergeFilters(PathFilter filter, PathFilter[] filters) {
    if (filters == null || filters.length == 0) {
      return filter;
    }

    int length = filters.length;
    PathFilter[] newFilters = Arrays.copyOf(filters, length + 1);
    newFilters[length] = filter;
    return mergeFilters(newFilters);
  }

  /**
   * Will merge given array of filters into one.
   * If given array of filters is empty, will return {@link #DUMMY_FILTER}.
   *
   * @param filters array of filters
   * @return one filter that combines all given filters
   */
  public static PathFilter mergeFilters(final PathFilter... filters) {
    if (filters.length == 0) {
      return DUMMY_FILTER;
    }

    return path -> Stream.of(filters).allMatch(filter -> filter.accept(path));
  }

  /**
   * Helper method that will store in given holder statuses of all directories present in given path applying custom filter.
   * If recursive flag is set to true, will call itself recursively to add statuses of nested directories.
   * If suppress exceptions flag is set to true, will ignore all exceptions during listing.
   *
   * @param fs current file system
   * @param path path to directory
   * @param recursive true if nested directories should be included
   * @param suppressExceptions indicates if exceptions should be ignored during listing
   * @param statuses holder for directory statuses
   * @param filter custom filter
   * @return holder with all matching directory statuses
   */
  private static List<FileStatus> listDirectories(FileSystem fs, Path path, boolean recursive, boolean suppressExceptions,
                                                  List<FileStatus> statuses, PathFilter filter) throws IOException {
    try {
      for (FileStatus status : fs.listStatus(path, filter)) {
        if (status.isDirectory()) {
          statuses.add(status);
          if (recursive) {
            listDirectories(fs, status.getPath(), true, suppressExceptions, statuses, filter);
          }
        }
      }
    } catch (Exception e) {
      if (suppressExceptions) {
        logger.debug("Exception during listing file statuses", e);
      } else {
        throw e;
      }
    }
    return statuses;
  }

  /**
   * Helper method that will store in given holder statuses of all files present in given path applying custom filter.
   * If recursive flag is set to true, will call itself recursively to add file statuses from nested directories.
   * If suppress exceptions flag is set to true, will ignore all exceptions during listing.
   *
   * @param fs current file system
   * @param path path to file or directory
   * @param recursive true if files in nested directories should be included
   * @param suppressExceptions indicates if exceptions should be ignored during listing
   * @param statuses holder for file statuses
   * @param filter custom filter
   * @return holder with all matching file statuses
   */
  private static List<FileStatus> listFiles(FileSystem fs, Path path, boolean recursive, boolean suppressExceptions,
                                            List<FileStatus> statuses, PathFilter filter) throws IOException {
    try {
      for (FileStatus status : fs.listStatus(path, filter)) {
        if (status.isDirectory()) {
          if (recursive) {
            listFiles(fs, status.getPath(), true, suppressExceptions, statuses, filter);
          }
        } else {
          statuses.add(status);
        }
      }
    } catch (Exception e) {
      if (suppressExceptions) {
        logger.debug("Exception during listing file statuses", e);
      } else {
        throw e;
      }
    }
    return statuses;
  }

  /**
   * Helper method that will store in given holder statuses of all directories and files present in given path applying custom filter.
   * If recursive flag is set to true, will call itself recursively to add nested directories and their file statuses.
   * If suppress exceptions flag is set to true, will ignore all exceptions during listing.
   *
   * @param fs current file system
   * @param path path to file or directory
   * @param recursive true if nested directories and their files should be included
   * @param suppressExceptions indicates if exceptions should be ignored during listing
   * @param statuses holder for directory and file statuses
   * @param filter custom filter
   * @return holder with all matching directory and file statuses
   */
  private static List<FileStatus> listAll(FileSystem fs, Path path, boolean recursive, boolean suppressExceptions,
                                          List<FileStatus> statuses, PathFilter filter) throws IOException {
    try {
      for (FileStatus status : fs.listStatus(path, filter)) {
        statuses.add(status);
        if (status.isDirectory() && recursive) {
          listAll(fs, status.getPath(), true, suppressExceptions, statuses, filter);
        }
      }
    } catch (Exception e) {
      if (suppressExceptions) {
        logger.debug("Exception during listing file statuses", e);
      } else {
        throw e;
      }
    }
    return statuses;
  }
}
