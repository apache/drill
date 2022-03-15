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

package org.apache.drill.exec.udfs.http;

import org.apache.drill.exec.vector.complex.reader.FieldReader;

import java.util.ArrayList;
import java.util.List;

public class HttpHelperUtils {

  /**
   * Accepts a list of input readers and converts that into an ArrayList of Strings
   * @param inputReaders The array of FieldReaders
   * @return A List of Strings containing the values from the FieldReaders.
   */
  public static List<String> buildParameterList(FieldReader[] inputReaders) {
    List<String> inputArguments = new ArrayList<>();

    // Skip the first argument because that is the input URL
    for (int i = 1; i < inputReaders.length; i++) {
      inputArguments.add(inputReaders[i].readObject().toString());
    }

    return inputArguments;
  }
}
