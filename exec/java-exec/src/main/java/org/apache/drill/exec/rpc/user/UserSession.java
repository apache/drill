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
package org.apache.drill.exec.rpc.user;

import java.io.IOException;

import net.hydromatic.optiq.SchemaPlus;

import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.store.SchemaFactory;

public class UserSession {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserSession.class);

  private DrillUser user;
  private String defaultSchema = "";
  private UserClientConnection connection;

  public UserSession(UserClientConnection connection, UserCredentials credentials, SchemaFactory factory) throws IOException{
    this.connection = connection;
  }


  public DrillUser getUser(){
    return user;
  }


  /**
   * Update the schema path for the session.
   * @param fullPath The desired path to set to.
   * @param schema The root schema to find this path within.
   * @return true if the path was set succesfully.  false if this path was unavailable.
   */
  public boolean setDefaultSchemaPath(String fullPath, SchemaPlus schema){
    SchemaPlus newDefault = getDefaultSchema(schema);
    if(newDefault == null) return false;
    this.defaultSchema = fullPath;
    return true;
  }

  public SchemaPlus getDefaultSchema(SchemaPlus rootSchema){
    String[] paths = defaultSchema.split("\\.");
    SchemaPlus schema = rootSchema;
    for(String p : paths){
      schema = schema.getSubSchema(p);
      if(schema == null) break;
    }
    return schema;
  }

  public boolean setSessionOption(String name, String value){
    return true;
  }
}
