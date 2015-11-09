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
package org.apache.drill.exec.vector.complex;

import org.apache.drill.exec.vector.AddOrGetResult;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VectorDescriptor;

/**
 * A mix-in used for introducing container vector-like behaviour.
 */
public interface ContainerVectorLike {

  /**
   * Creates and adds a child vector if none with the same name exists, else returns the vector instance.
   *
   * @param  descriptor vector descriptor
   * @return  result of operation wrapping vector corresponding to the given descriptor and whether it's newly created
   * @throws org.apache.drill.common.exceptions.DrillRuntimeException
   *    if schema change is not permissible between the given and existing data vector types.
   */
  <T extends ValueVector> AddOrGetResult<T> addOrGetVector(VectorDescriptor descriptor);

  /**
   * Returns the number of child vectors in this container vector-like instance.
   */
  int size();
}
