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
package org.apache.drill.exec.planner.sql.handlers;

import java.sql.Timestamp;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class ShowFilesCommandResult {

  /* Fields that will be returned as columns
   * for a 'SHOW FILES' command
   */

  // Name of the file
  public String name;

  // Is it a directory
  public boolean isDirectory;

  // Is it a file
  public boolean isFile;

  // Length of the file
  public long length;

  // File owner
  public String owner;

  // File group
  public String group;

  // File permissions
  public String permissions;

  // Access Time
  public Timestamp accessTime;

  // Modification Time
  public Timestamp modificationTime;

  public ShowFilesCommandResult(String name,
                                boolean isDirectory,
                                boolean isFile,
                                long length,
                                String owner,
                                String group,
                                String permissions,
                                long accessTime,
                                long modificationTime) {
    this.name = name;
    this.isDirectory = isDirectory;
    this.isFile = isFile;
    this.length = length;
    this.owner = owner;
    this.group = group;
    this.permissions = permissions;

    // Get the timestamp in UTC because Drill's internal TIMESTAMP stores time in UTC
    DateTime at = new DateTime(accessTime).withZoneRetainFields(DateTimeZone.UTC);
    this.accessTime = new Timestamp(at.getMillis());

    DateTime mt = new DateTime(modificationTime).withZoneRetainFields(DateTimeZone.UTC);
    this.modificationTime = new Timestamp(mt.getMillis());
  }
}
