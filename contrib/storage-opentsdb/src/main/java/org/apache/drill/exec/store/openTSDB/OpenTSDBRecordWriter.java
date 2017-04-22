/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.openTSDB;

import lombok.extern.slf4j.Slf4j;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.store.AbstractRecordWriter;

import java.io.IOException;
import java.util.Map;

@Slf4j
public class OpenTSDBRecordWriter extends AbstractRecordWriter {

    @Override
    public void init(Map<String, String> writerOptions) throws IOException {

    }

    @Override
    public void updateSchema(VectorAccessible batch) throws IOException {

    }

    @Override
    public void startRecord() throws IOException {

    }

    @Override
    public void endRecord() throws IOException {

    }

    @Override
    public void abort() throws IOException {

    }

    @Override
    public void cleanup() throws IOException {

    }
}
