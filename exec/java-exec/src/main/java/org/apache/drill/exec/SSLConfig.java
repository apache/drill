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
package org.apache.drill.exec;

import com.typesafe.config.Config;
import org.apache.drill.common.exceptions.DrillException;

public class SSLConfig {

  private final String keystorePath;

  private final String keystorePassword;

  private final String truststorePath;

  private final String truststorePassword;


  public SSLConfig(Config config) throws DrillException {

    keystorePath = config.getString(ExecConstants.HTTP_KEYSTORE_PATH).trim();

    keystorePassword = config.getString(ExecConstants.HTTP_KEYSTORE_PASSWORD).trim();

    truststorePath = config.getString(ExecConstants.HTTP_TRUSTSTORE_PATH).trim();

    truststorePassword = config.getString(ExecConstants.HTTP_TRUSTSTORE_PASSWORD).trim();

    /*If keystorePath or keystorePassword is provided in the configuration file use that*/
    if (!keystorePath.isEmpty() || !keystorePassword.isEmpty()) {
      if (keystorePath.isEmpty()) {
        throw new DrillException(" *.ssl.keyStorePath in the configuration file is empty, but *.ssl.keyStorePassword is set");
      }
      else if (keystorePassword.isEmpty()){
        throw new DrillException(" *.ssl.keyStorePassword in the configuration file is empty, but *.ssl.keyStorePath is set ");
      }

    }
  }

  public boolean isSslValid() {  return !keystorePath.isEmpty() && !keystorePassword.isEmpty(); }

  public String getKeyStorePath() {  return keystorePath; }

  public String getKeyStorePassword() {  return keystorePassword; }

  public boolean hasTrustStorePath() {  return !truststorePath.isEmpty(); }

  public boolean hasTrustStorePassword() {  return !truststorePassword.isEmpty(); }

  public String getTrustStorePath() {  return truststorePath; }

  public String getTrustStorePassword() {  return truststorePassword; }
}