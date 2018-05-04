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

/**
 * Helper class that provides methods to list directories or file or both statuses.
 * Can list statuses recursive and apply custom filters.
 */
public class FileSystemUtil {

  /**
   * Filter that will accept all files and directories.
   */
  public static final PathFilter DUMMY_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      return true;
    }
  };

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
    listDirectories(fs, path, recursive, statuses, mergeFilters(filters));
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
    listFiles(fs, path, recursive, statuses, mergeFilters(filters));
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
    listAll(fs, path, recursive, statuses, mergeFilters(filters));
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

    return new PathFilter() {
      @Override
      public boolean accept(Path path) {
        for (PathFilter filter : filters) {
          if (!filter.accept(path)) {
            return false;
          }
        }
        return true;
      }
    };
  }

  /**
   * Helper method that will rename/move file specified in the source path to a destination path
   *
   * @param fs current file system
   * @param src path to source
   * @param dst path to destination
   * @return status of rename/move
   */
  public static boolean rename(FileSystem fs, Path src, Path dst) throws IOException {
    return fs.rename(src, dst);
  }

  /**
   * Helper method that will store in given holder statuses of all directories present in given path applying custom filter.
   * If recursive flag is set to true, will call itself recursively to add statuses of nested directories.
   *
   * @param fs current file system
   * @param path path to directory
   * @param recursive true if nested directories should be included
   * @param statuses holder for directory statuses
   * @param filter custom filter
   * @return holder with all matching directory statuses
   */
  private static List<FileStatus> listDirectories(FileSystem fs, Path path, boolean recursive, List<FileStatus> statuses, PathFilter filter) throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(path, filter);
    for (FileStatus status: fileStatuses) {
      if (status.isDirectory()) {
        statuses.add(status);
        if (recursive) {
          listDirectories(fs, status.getPath(), true, statuses, filter);
        }
      }
    }
    return statuses;
  }

  /**
   * Helper method that will store in given holder statuses of all files present in given path applying custom filter.
   * If recursive flag is set to true, will call itself recursively to add file statuses from nested directories.
   *
   * @param fs current file system
   * @param path path to file or directory
   * @param recursive true if files in nested directories should be included
   * @param statuses holder for file statuses
   * @param filter custom filter
   * @return holder with all matching file statuses
   */
  private static List<FileStatus> listFiles(FileSystem fs, Path path, boolean recursive, List<FileStatus> statuses, PathFilter filter) throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(path, filter);
    for (FileStatus status: fileStatuses) {
      if (status.isDirectory()) {
        if (recursive) {
          listFiles(fs, status.getPath(), true, statuses, filter);
        }
      } else {
        statuses.add(status);
      }
    }
    return statuses;
  }

  /**
   * Helper method that will store in given holder statuses of all directories and files present in given path applying custom filter.
   * If recursive flag is set to true, will call itself recursively to add nested directories and their file statuses.
   *
   * @param fs current file system
   * @param path path to file or directory
   * @param recursive true if nested directories and their files should be included
   * @param statuses holder for directory and file statuses
   * @param filter custom filter
   * @return holder with all matching directory and file statuses
   */
  private static List<FileStatus> listAll(FileSystem fs, Path path, boolean recursive, List<FileStatus> statuses, PathFilter filter) throws IOException {
    for (FileStatus status: fs.listStatus(path, filter)) {
      statuses.add(status);
      if (status.isDirectory() && recursive) {
        listAll(fs, status.getPath(), true, statuses, filter);
      }
    }
    return statuses;
  }
}
