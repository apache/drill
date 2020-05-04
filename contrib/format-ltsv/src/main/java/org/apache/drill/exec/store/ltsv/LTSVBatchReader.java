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

package org.apache.drill.exec.store.ltsv;

import com.github.lolo.ltsv.LtsvParser;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.store.easy.EasyEVFBatchReader;

import java.util.Iterator;
import java.util.Map;


public class LTSVBatchReader extends EasyEVFBatchReader {

  private final LTSVReaderConfig readerConfig;

  static class LTSVReaderConfig {
    final LTSVFormatPlugin plugin;

    final LTSVFormatPluginConfig config;

    LTSVReaderConfig(LTSVFormatPlugin plugin) {
      this.plugin = plugin;
      this.config = plugin.getConfig();
    }
  }

  public LTSVBatchReader(LTSVReaderConfig readerConfig) {
    this.readerConfig = readerConfig;
  }

  public boolean open(FileSchemaNegotiator negotiator) {
    super.open(negotiator);

    LtsvParser.Builder builder = LtsvParser.builder();

    // Set Parser Options
    if (readerConfig.config.getLenientMode()) {
      builder = builder.lenient();
    }

    if (! isEmpty(readerConfig.config.getQuoteCharacter())) {
      builder = builder.withQuoteChar(readerConfig.config.getQuoteCharacter());
    }

    if (! isEmpty(readerConfig.config.getKvDelimiter())) {
      builder = builder.withKvDelimiter(readerConfig.config.getKvDelimiter());
    }

    if (! isEmpty(readerConfig.config.getEscapeCharacter())) {
      builder = builder.withEscapeChar(readerConfig.config.getEscapeCharacter());
    }

    if (! isEmpty(readerConfig.config.getEntryDelimiter())) {
      builder = builder.withEntryDelimiter(readerConfig.config.getEntryDelimiter());
    }

    if (! isEmpty(readerConfig.config.getLineEnding())) {
      builder = builder.withLineEnding(readerConfig.config.getLineEnding());
    }

    LtsvParser parser = builder.build();
    Iterator<Map<String, String>> ltsvIterator = parser.parse(fsStream);
    super.fileIterator = new LTSVRecordIterator(getRowWriter(), ltsvIterator, errorContext);
    return true;
  }

  private boolean isEmpty(char c) {
    return c == '\u0000';
  }
}
