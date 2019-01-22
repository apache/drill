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
/**
 * Defines the scan operation implementation. The scan operator is a generic mechanism
 * that fits into the Drill Volcano-based iterator protocol to return record batches
 * from one or more readers.
 * <p>
 * Two versions of the scan operator exist:<ul>
 * <li>{@link ScanBatch}: the original version that uses readers based on the
 * {@link RecordReader} interface. <tt>ScanBatch</tt> cannot, however, handle
 * limited-length vectors.</li>
 * <li>{@link ScanOperatorExec}: the revised version that uses a more modular
 * design and that offers a mutator that is a bit easier to use, and can limit
 * vector sizes.</li></ul>
 * New code should use the new version, existing code will continue to use the
 * <tt>ScanBatch</tt> version until all readers are converted to the new format.
 * <p>
 * Further, the new version is designed to allow intensive unit test without
 * the need for the Drill server. New readers should exploit this feature to
 * include intensive tests to keep Drill quality high.
 * <p>
 * See {@link ScanOperatorExec} for details of the scan operator protocol
 * and components.
 * <p>
 * See the "managed" package for a reusable framework for handling the
 * details of batches, schema and so on.
 */

package org.apache.drill.exec.physical.impl.scan;
