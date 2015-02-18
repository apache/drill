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
package org.apache.drill.exec.store.parquet.columnreaders;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestNullableFixedAlignedReaders extends BaseTestQuery {

  /**
   * check if NullableDateReader works well with dictionary encoding.
   */
  @Test
  public void testNullableDateReaderWithDictionary() throws Exception {
    // the file 'date_dictionary.parquet' contains one single DATE column with 600 rows and 290 distinct date values
    // and uses the PLAIN_DICTIONARY encoder

    // query parquet file. We shouldn't get any exception
    testNoResult("SELECT * FROM cp.`parquet/date_dictionary.parquet`");
  }
}
