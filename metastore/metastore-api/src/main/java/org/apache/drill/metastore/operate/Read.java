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
package org.apache.drill.metastore.operate;

import org.apache.drill.metastore.expressions.FilterExpression;

import java.util.Arrays;
import java.util.List;

/**
 * Drill Metastore Read interface contains methods to be implemented in order
 * to provide read functionality from the Metastore component.
 *
 * @param <T> component unit type
 */
public interface Read<T> {

  /**
   * Provides filter expression by which metastore component data will be filtered.
   * If filter expression is not indicated, all Metastore component data will be read.
   *
   * @param filter filter expression
   * @return current instance of Read interface implementation
   */
  Read<T> filter(FilterExpression filter);

  /**
   * Provides list of columns to be read from Metastore component.
   * If no columns are indicated, all columns will be read.
   * Depending on Metastore component implementation, providing list of columns to be read,
   * can improve retrieval performance.
   *
   * @param columns list of columns to be read from Metastore component
   * @return current instance of Read interface implementation
   */
  Read<T> columns(List<String> columns);

  default Read<T> columns(String... columns) {
    return columns(Arrays.asList(columns));
  }

  /**
   * Executes read operation from Metastore component, returns obtained result in a form
   * of list of component units  which later can be transformed into suitable format.
   *
   * @return list of component units
   */
  List<T> execute();
}
