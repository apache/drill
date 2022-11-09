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

package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.vector.complex.impl.SingleMapReaderImpl;
import org.apache.drill.exec.vector.complex.reader.FieldReader;

public class IsEmptyUtils {

  /**
   * This function recursively traverses a Drill map to determine whether the map is empty or not.
   * @param reader A {@link FieldReader} containing the field in question
   * @return True if the field contains no data, false if it does.
   */
  public static boolean mapIsEmpty(FieldReader reader) {

    if (reader.getType().getMinorType() == MinorType.MAP) {
      SingleMapReaderImpl mapReader = (SingleMapReaderImpl) reader;

      // If the map reader has no fields return true
      if (!mapReader.iterator().hasNext()) {
        return true;
      }
      // Now check to see whether the fields contain anything
      boolean isEmpty = true;
      for (String fieldName : reader) {
        FieldReader fieldReader = reader.reader(fieldName);
        if (fieldReader.isSet() && fieldReader.getType().getMinorType() == MinorType.MAP) {
          boolean innerMapIsEmpty = mapIsEmpty(fieldReader);

          if (!innerMapIsEmpty) {
            isEmpty = false;
          }

        } else if (fieldReader.isSet()) {
          isEmpty = false;
        }
      }
      return isEmpty;
    } else {
      return ! reader.isSet();
    }
  }
}
