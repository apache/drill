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
package org.apache.drill.exec.vector.complex.fn;

/**
 * Based on
 * <a href="https://docs.mongodb.com/manual/reference/mongodb-extended-json-v1/">
 * V1</a> of the Mongo extended type spec. Some names overlap with the current
 * <a href="https://docs.mongodb.com/manual/reference/mongodb-extended-json/">
 * V2</a> of the Mongo specs.
 */
public interface ExtendedTypeName {
  String BINARY = "$binary";      // base64 encoded binary (ZHJpbGw=)  [from Mongo]
  String TYPE = "$type";          // type of binary data
  String DATE = "$dateDay";       // ISO date with no time. such as (12-24-27)
  String TIME = "$time";          // ISO time with no timezone (19:20:30.45Z)
  String TIMESTAMP = "$date";     // ISO standard time (2009-02-23T00:00:00.000-08:00) [from Mongo]
  String INTERVAL = "$interval";  // ISO standard duration (PT26.4S)
  String INTEGER = "$numberLong"; // 8 byte signed integer (123) [from Mongo]
  String DECIMAL = "$decimal";    // exact numeric value (123.123)
}

