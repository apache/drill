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
package org.apache.drill.exec.store.mpjdbc;

public class MPJdbcClientOptions {
  private String driver;
  private String user;
  private String passwd;

  public MPJdbcClientOptions(String driver, String user, String passwd) {
    this.driver = driver;
    this.user = user;
    this.passwd = passwd;
  }

  public MPJdbcClientOptions(MPJdbcFormatConfig storageConfig) {
    this.driver = storageConfig.getDriver();
    this.user = storageConfig.getUser();
    this.passwd = storageConfig.getPasswd();
  }

  public String getDriver() {
    // TODO Auto-generated method stub
    return this.driver;
  }

  public String getUser() {
    // TODO Auto-generated method stub
    return this.user;
  }

  public String getPassword() {
    // TODO Auto-generated method stub
    return this.passwd;
  }

}
