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
 * Defines the type conversion mechanism along with many basic
 * conversions.
 * <p>
 * The provided conversions all handle the normal cases. Exceptional
 * case (overflow, ambiguous formats) are handled according to Java
 * rules.
 * <p>
 * The {@link StandardConversions} class defines the types of conversions
 * needed:
 * <ul>
 * <li>None: Converting from a type to itself.</li>
 * <li>Implicit: Conversion is done by the column writers, such as
 * converting from an INT to a SMALLINT.</li>
 * <li>Explicit: Requires a converter. If an unambiguous conversion is
 * possible, that converter should occur here. If conversion is ambiguous,
 * or a reader needs to support a special format, then the reader can add
 * custom conversions for these cases.</li>
 * </ul>
 * <p>
 * Would be good to validate each conversion against the corresponding CAST
 * operation. In an ideal world, all conversions, normal and exceptional,
 * will work the same as either a CAST (where the operations is handled by
 * the Project operator via code generation) and the standard conversions.
 */
package org.apache.drill.exec.vector.accessor.convert;
