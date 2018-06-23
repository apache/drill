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
package org.apache.drill.exec.store.easy.text.compliant;

import java.io.IOException;

import com.univocity.parsers.common.ParsingContext;

class TextParsingContext implements ParsingContext {

  private final TextInput input;
  private final TextOutput output;
  protected boolean stopped = false;

  private int[] extractedIndexes = null;

  public TextParsingContext(TextInput input, TextOutput output) {
    this.input = input;
    this.output = output;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop() {
    stopped = true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isStopped() {
    return stopped;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long currentLine() {
    return input.lineCount();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long currentChar() {
    return input.charCount();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int currentColumn() {
    return -1;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String[] headers() {
    return new String[]{};
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int[] extractedFieldIndexes() {
    return extractedIndexes;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long currentRecord() {
    return output.getRecordCount();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String currentParsedContent() {
    try {
      return input.getStringSinceMarkForError();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void skipLines(int lines) {
  }

  @Override
  public boolean columnsReordered() {
    return false;
  }
}

