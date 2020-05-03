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


public class LTSVBatchReader extends EasyEVFBatchReader {
  public boolean open(FileSchemaNegotiator negotiator) {
    super.open(negotiator);
    LtsvParser parser = LtsvParser.builder().withQuoteChar('`').withKvDelimiter('=').build();
    super.fileIterator = new LTSVRecordIterator(getRowWriter(), reader, parser, errorContext);
    return true;
  }
}
