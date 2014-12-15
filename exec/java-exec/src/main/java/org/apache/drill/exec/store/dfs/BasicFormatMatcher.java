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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;

public class BasicFormatMatcher extends FormatMatcher{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicFormatMatcher.class);

  private final List<Pattern> patterns;
  private final MagicStringMatcher matcher;
  protected final DrillFileSystem fs;
  protected final FormatPlugin plugin;
  protected final boolean compressible;
  protected final CompressionCodecFactory codecFactory;

  public BasicFormatMatcher(FormatPlugin plugin, DrillFileSystem fs, List<Pattern> patterns, List<MagicString> magicStrings) {
    super();
    this.patterns = ImmutableList.copyOf(patterns);
    this.matcher = new MagicStringMatcher(magicStrings);
    this.fs = fs;
    this.plugin = plugin;
    this.compressible = false;
    this.codecFactory = null;
  }

  public BasicFormatMatcher(FormatPlugin plugin, DrillFileSystem fs, List<String> extensions, boolean compressible) {
    List<Pattern> patterns = Lists.newArrayList();
    for (String extension : extensions) {
      patterns.add(Pattern.compile(".*\\." + extension));
    }
    this.patterns = patterns;
    this.matcher = new MagicStringMatcher(new ArrayList<MagicString>());
    this.fs = fs;
    this.plugin = plugin;
    this.compressible = compressible;
    this.codecFactory = new CompressionCodecFactory(fs.getConf());
  }

  @Override
  public boolean supportDirectoryReads() {
    return false;
  }

  @Override
  public FormatSelection isReadable(FileSelection selection) throws IOException {
    if (isReadable(selection.getFirstPath(fs))) {
      if (plugin.getName() != null) {
        NamedFormatPluginConfig namedConfig = new NamedFormatPluginConfig();
        namedConfig.name = plugin.getName();
        return new FormatSelection(namedConfig, selection);
      } else {
        return new FormatSelection(plugin.getConfig(), selection);
      }
    }
    return null;
  }

  protected final boolean isReadable(FileStatus status) throws IOException {
    CompressionCodec codec = null;
    if (compressible) {
      codec = codecFactory.getCodec(status.getPath());
    }
    String fileName;
    if (codec != null) {
      String path = status.getPath().toString();
      fileName = path.substring(0, path.lastIndexOf('.'));
    } else {
      fileName = status.getPath().toString();
    }
    for (Pattern p : patterns) {
      if (p.matcher(fileName).matches()) {
        return true;
      }
    }

    if (matcher.matches(status)) {
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

    public boolean matches(FileStatus status) throws IOException{
      if (ranges.isEmpty()) {
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
