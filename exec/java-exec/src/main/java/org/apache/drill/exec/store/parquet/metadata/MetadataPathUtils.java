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
package org.apache.drill.exec.store.parquet.metadata;

import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.common.util.DrillVersionInfo;
import org.apache.hadoop.fs.Path;

import java.util.List;

import static org.apache.drill.exec.store.parquet.metadata.MetadataVersion.Constants.SUPPORTED_VERSIONS;
import static org.apache.drill.exec.store.parquet.metadata.Metadata_V3.ParquetFileMetadata_v3;
import static org.apache.drill.exec.store.parquet.metadata.Metadata_V3.ParquetTableMetadata_v3;

/**
 * Util class that contains helper methods for converting paths in the table and directory metadata structures
 */
public class MetadataPathUtils {

  /**
   * Helper method that converts a list of relative paths to absolute ones
   *
   * @param paths list of relative paths
   * @param baseDir base parent directory
   * @return list of absolute paths
   */
  public static List<String> convertToAbsolutePaths(List<String> paths, String baseDir) {
    if (!paths.isEmpty()) {
      List<String> absolutePaths = Lists.newArrayList();
      for (String relativePath : paths) {
        String absolutePath = (new Path(relativePath).isAbsolute()) ? relativePath
            : new Path(baseDir, relativePath).toUri().getPath();
        absolutePaths.add(absolutePath);
      }
      return absolutePaths;
    }
    return paths;
  }

  /**
   * Convert a list of files with relative paths to files with absolute ones
   *
   * @param files list of files with relative paths
   * @param baseDir base parent directory
   * @return list of files with absolute paths
   */
  public static List<ParquetFileMetadata_v3> convertToFilesWithAbsolutePaths(
      List<ParquetFileMetadata_v3> files, String baseDir) {
    if (!files.isEmpty()) {
      List<ParquetFileMetadata_v3> filesWithAbsolutePaths = Lists.newArrayList();
      for (ParquetFileMetadata_v3 file : files) {
        Path relativePath = new Path(file.getPath());
        // create a new file if old one contains a relative path, otherwise use an old file
        ParquetFileMetadata_v3 fileWithAbsolutePath = (relativePath.isAbsolute()) ? file
            : new ParquetFileMetadata_v3(new Path(baseDir, relativePath).toUri().getPath(), file.length, file.rowGroups);
        filesWithAbsolutePaths.add(fileWithAbsolutePath);
      }
      return filesWithAbsolutePaths;
    }
    return files;
  }

  /**
   * Creates a new parquet table metadata from the {@code tableMetadataWithAbsolutePaths} parquet table.
   * A new parquet table will contain relative paths for the files and directories.
   *
   * @param tableMetadataWithAbsolutePaths parquet table metadata with absolute paths for the files and directories
   * @param baseDir base parent directory
   * @return parquet table metadata with relative paths for the files and directories
   */
  public static ParquetTableMetadata_v3 createMetadataWithRelativePaths(
      ParquetTableMetadata_v3 tableMetadataWithAbsolutePaths, String baseDir) {
    List<String> directoriesWithRelativePaths = Lists.newArrayList();
    for (String directory : tableMetadataWithAbsolutePaths.getDirectories()) {
      directoriesWithRelativePaths.add(relativize(baseDir, directory));
    }
    List<ParquetFileMetadata_v3> filesWithRelativePaths = Lists.newArrayList();
    for (ParquetFileMetadata_v3 file : tableMetadataWithAbsolutePaths.files) {
      filesWithRelativePaths.add(new ParquetFileMetadata_v3(
          relativize(baseDir, file.getPath()), file.length, file.rowGroups));
    }
    return new ParquetTableMetadata_v3(SUPPORTED_VERSIONS.last().toString(), tableMetadataWithAbsolutePaths,
        filesWithRelativePaths, directoriesWithRelativePaths, DrillVersionInfo.getVersion());
  }

  /**
   * Constructs relative path from child full path and base path. Or return child path if the last one is already relative
   *
   * @param childPath full absolute path
   * @param baseDir base path (the part of the Path, which should be cut off from child path)
   * @return relative path
   */
  public static String relativize(String baseDir, String childPath) {
    Path fullPathWithoutSchemeAndAuthority = Path.getPathWithoutSchemeAndAuthority(new Path(childPath));
    Path basePathWithoutSchemeAndAuthority = Path.getPathWithoutSchemeAndAuthority(new Path(baseDir));

    // Since hadoop Path hasn't relativize() we use uri.relativize() to get relative path
    Path relativeFilePath = new Path(basePathWithoutSchemeAndAuthority.toUri()
        .relativize(fullPathWithoutSchemeAndAuthority.toUri()));
    if (relativeFilePath.isAbsolute()) {
      throw new IllegalStateException(String.format("Path %s is not a subpath of %s.",
          basePathWithoutSchemeAndAuthority.toUri().getPath(), fullPathWithoutSchemeAndAuthority.toUri().getPath()));
    }
    return relativeFilePath.toUri().getPath();
  }

}
