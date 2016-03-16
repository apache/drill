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
package org.apache.drill.exec.store.parquet;

public class ParquetReaderStats {

  public long numDictPageHeaders;
  public long numPageHeaders;
  public long numDictPageLoads;
  public long numPageLoads;
  public long numDictPagesDecompressed;
  public long numPagesDecompressed;

  public long totalDictPageHeaderBytes;
  public long totalPageHeaderBytes;
  public long totalDictPageReadBytes;
  public long totalPageReadBytes;
  public long totalDictDecompressedBytes;
  public long totalDecompressedBytes;

  public long timeDictPageHeaders;
  public long timePageHeaders;
  public long timeDictPageLoads;
  public long timePageLoads;
  public long timeDictPagesDecompressed;
  public long timePagesDecompressed;

  public ParquetReaderStats() {
  }

}


