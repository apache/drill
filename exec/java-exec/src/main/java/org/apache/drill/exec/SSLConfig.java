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
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SSLConfig.class);

  private final String keystorePath;

  private final String keystorePassword;

  private final String truststorePassword;

  private final String truststorePath;

  public boolean isValid = false;

  public SSLConfig(Config config) throws DrillException {

    keystorePath = config.getString(ExecConstants.HTTP_KEYSTORE_PATH).trim();

    keystorePassword = config.getString(ExecConstants.HTTP_KEYSTORE_PASSWORD).trim();

    truststorePath = config.getString(ExecConstants.HTTP_TRUSTSTORE_PATH).trim();

    truststorePassword = config.getString(ExecConstants.HTTP_TRUSTSTORE_PASSWORD).trim();

    /*If keystorePath or keystorePassword is provided in the configuration file use that*/
    if (!keystorePath.isEmpty() || !keystorePassword.isEmpty()) {
      if (keystorePath.isEmpty() || keystorePassword.isEmpty()) {
        throw new DrillException(" *.ssl.keyStorePath and/or *.ssl.keyStorePassword in the configuration file can't be empty.");
      }
      isValid = true;
    }
  }

  public String getkeystorePath() {
    return keystorePath;
  }

  public String getkeystorePassword() {
    return keystorePassword;
  }

  public String gettruststorePath() {
    return truststorePath;
  }

  public String gettruststorePassword() {
    return truststorePassword;
  }
}
