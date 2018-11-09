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
 * This package contains an Apache Drill format plugin capable of reading
 * msgpack files. <br>
 * The specification for the msgpack file format is found at
 * <a href="https://github.com/msgpack/msgpack/blob/master/spec.md">msgpack
 * spec</a> <br>
 * This format plubin uses a Java library to read the msgpack files.
 * <a href="https://github.com/msgpack/msgpack-java">msgpack-java</a> This
 * library is capable of reading all of the msgpack data types
 * <ul>
 * <li>nil format
 * <li>bool format family
 * <li>int format family
 * <li>float format family
 * <li>str format family
 * <li>bin format family
 * <li>array format family
 * <li>map format family
 * <li>ext format family
 * </ul>
 * <br>
 * The ext format provides a means of providing third party data encodings. For
 * example msgpack defines an extended type for timestamps. <a href=
 * "https://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type">
 * timestamp extension type</a> <br>
 * This msgpack format plugin implements the timestamp extended type via the
 * class
 * {@link org.apache.drill.exec.store.msgpack.valuewriter.TimestampValueWriter}
 * It's an implementation of
 * {@link org.apache.drill.exec.store.msgpack.valuewriter.ExtensionValueWriter}.
 * These implemenations are discovered via the Java Plugin Service.
 */
package org.apache.drill.exec.store.msgpack;
