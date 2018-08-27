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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Range;

public class BasicFormatMatcher extends FormatMatcher {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicFormatMatcher.class);

  protected final FormatPlugin plugin;
  protected final boolean compressible;
  protected final CompressionCodecFactory codecFactory;

  private final List<Pattern> patterns;
  private final MagicStringMatcher matcher;

  public BasicFormatMatcher(FormatPlugin plugin, List<Pattern> patterns, List<MagicString> magicStrings) {
    super();
    this.patterns = ImmutableList.copyOf(patterns);
    this.matcher = new MagicStringMatcher(magicStrings);
    this.plugin = plugin;
    this.compressible = false;
    this.codecFactory = null;
  }

  public BasicFormatMatcher(FormatPlugin plugin, Configuration fsConf, List<String> extensions, boolean compressible) {
    List<Pattern> patterns = Lists.newArrayList();
    for (String extension : extensions) {
      patterns.add(Pattern.compile(".*\\." + extension));
    }
    this.patterns = patterns;
    this.matcher = new MagicStringMatcher(new ArrayList<MagicString>());
    this.plugin = plugin;
    this.compressible = compressible;
    this.codecFactory = new CompressionCodecFactory(fsConf);
  }

  @Override
  public boolean supportDirectoryReads() {
    return false;
  }

  @Override
  public DrillTable isReadable(DrillFileSystem fs,
      FileSelection selection, FileSystemPlugin fsPlugin,
      String storageEngineName, SchemaConfig schemaConfig) throws IOException {
    if (isFileReadable(fs, selection.getFirstPath(fs))) {
      return new DynamicDrillTable(fsPlugin, storageEngineName, schemaConfig.getUserName(),
          new FormatSelection(plugin.getConfig(), selection));
    }
    return null;
  }

  /*
   * Function returns true if the file extension matches the pattern
   */
  @Override
  public boolean isFileReadable(DrillFileSystem fs, FileStatus status) throws IOException {
  CompressionCodec codec = null;
    if (compressible) {
      codec = codecFactory.getCodec(status.getPath());
    }
    String fileName = status.getPath().toString();
    String fileNameHacked = null;
    if (codec != null) {
        fileNameHacked = fileName.substring(0, fileName.lastIndexOf('.'));
    }

    // Check for a matching pattern for compressed and uncompressed file name
    for (Pattern p : patterns) {
      if (p.matcher(fileName).matches()) {
        return true;
      }
      if (fileNameHacked != null  &&  p.matcher(fileNameHacked).matches()) {
        return true;
      }
    }

    if (matcher.matches(fs, status)) {
      return true;
    }
    return false;
  }

  @Override
  @JsonIgnore
  public FormatPlugin getFormatPlugin() {
    return plugin;
  }


  private class MagicStringMatcher {

    private List<RangeMagics> ranges;

    public MagicStringMatcher(List<MagicString> magicStrings) {
      ranges = Lists.newArrayList();
      for(MagicString ms : magicStrings) {
        ranges.add(new RangeMagics(ms));
      }
    }

    public boolean matches(DrillFileSystem fs, FileStatus status) throws IOException{
      if (ranges.isEmpty() || status.isDirectory()) {
        return false;
      }
      // walk all the way down in the symlinks until a hard entry is reached
      FileStatus current = status;
      while (current.isSymlink()) {
        current = fs.getFileStatus(status.getSymlink());
      }
      // if hard entry is not a file nor can it be a symlink then it is not readable simply deny matching.
      if (!current.isFile()) {
        return false;
      }

      final Range<Long> fileRange = Range.closedOpen( 0L, status.getLen());

      try (FSDataInputStream is = fs.open(status.getPath())) {
        for(RangeMagics rMagic : ranges) {
          Range<Long> r = rMagic.range;
          if (!fileRange.encloses(r)) {
            continue;
          }
          int len = (int) (r.upperEndpoint() - r.lowerEndpoint());
          byte[] bytes = new byte[len];
          is.readFully(r.lowerEndpoint(), bytes);
          for (byte[] magic : rMagic.magics) {
            if (Arrays.equals(magic, bytes)) {
              return true;
            }
          }
        }
      }
      return false;
    }

    private class RangeMagics{
      Range<Long> range;
      byte[][] magics;

      public RangeMagics(MagicString ms) {
        this.range = Range.closedOpen( ms.getOffset(), (long) ms.getBytes().length);
        this.magics = new byte[][]{ms.getBytes()};
      }
    }
  }

}
