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

package org.apache.drill.exec.store.splunk;

import com.splunk.EntityCollection;
import com.splunk.HttpService;
import com.splunk.Index;
import com.splunk.SSLSecurityProtocol;
import com.splunk.Service;
import com.splunk.ServiceArgs;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.Optional;

/**
 * This class wraps the functionality of the Splunk connection for Drill.
 */
public class SplunkConnection {

  private static final Logger logger = LoggerFactory.getLogger(SplunkConnection.class);

  private final Optional<UsernamePasswordCredentials> credentials;
  private final String scheme;
  private final String hostname;
  private final Integer port;
  // Whether the Splunk client will validate the server's SSL cert.
  private final boolean validateCertificates;
  // The application context of the service.
  private final String app;
  // The owner context of the service.
  private final String owner;
  // A Splunk authentication token to use for the session.
  private final String token;
  // A valid login cookie.
  private final String cookie;

  private Service service;
  private int connectionAttempts;

  public SplunkConnection(SplunkPluginConfig config, String queryUserName) {
    if (config.getAuthMode() == AuthMode.USER_TRANSLATION) {
      this.credentials = config.getUsernamePasswordCredentials(queryUserName);
    } else {
      this.credentials = config.getUsernamePasswordCredentials();
    }
    this.scheme = config.getScheme();
    this.hostname = config.getHostname();
    this.port = config.getPort();
    this.app = config.getApp();
    this.owner = config.getOwner();
    this.token = config.getToken();
    this.cookie = config.getCookie();
    this.validateCertificates = Optional.ofNullable(config.getValidateCertificates()).orElse(false);
    this.connectionAttempts = config.getReconnectRetries();
    service = connect();
  }

  /**
   * This constructor is used for testing only
   */
  public SplunkConnection(SplunkPluginConfig config, Service service, String queryUserName) {
    if (config.getAuthMode() == AuthMode.USER_TRANSLATION) {
      this.credentials = config.getUsernamePasswordCredentials(queryUserName);
    } else {
      this.credentials = config.getUsernamePasswordCredentials();
    }
    this.scheme = config.getScheme();
    this.hostname = config.getHostname();
    this.port = config.getPort();
    this.app = config.getApp();
    this.owner = config.getOwner();
    this.token = config.getToken();
    this.cookie = config.getCookie();
    this.validateCertificates = config.getValidateCertificates();
    this.service = service;
  }

  /**
   * Connects to Splunk instance
   * @return an active Splunk connection.
   */
  public Service connect() {
    HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2);
    HttpService.setValidateCertificates(validateCertificates);

    ServiceArgs loginArgs = new ServiceArgs();
    if (scheme != null) {
      // Fall back to the Splunk SDK default if our value is null by not setting
      loginArgs.setScheme(scheme);
    }
    if (hostname != null) {
      // Fall back to the Splunk SDK default if our value is null by not setting
      loginArgs.setHost(hostname);
    }
    if (port != null) {
      // Fall back to the Splunk SDK default if our value is null by not setting
      loginArgs.setPort(port);
    }
    loginArgs.setPassword(credentials.map(UsernamePasswordCredentials::getPassword).orElse(null));
    loginArgs.setUsername(credentials.map(UsernamePasswordCredentials::getUsername).orElse(null));
    loginArgs.setApp(app);
    loginArgs.setOwner(owner);
    loginArgs.setToken(token);
    loginArgs.setCookie(cookie);

     try {
      connectionAttempts--;
      service = Service.connect(loginArgs);
    } catch (Exception e) {
      if (connectionAttempts > 0) {
        try {
          TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException interruptedException) {
          logger.error("Unable to wait 2 secs before next connection try to Splunk");
        }
        return connect();
      }
      throw UserException
        .connectionError(e)
        .message("Unable to connect to Splunk at %s:%s", hostname, port)
        .build(logger);
    }
    logger.info("Successfully connected to {} on port {}", hostname, port);
    return service;
  }

  /**
   * Gets the available indexes from Splunk. Drill treats these as a table.
   * @return A collection of Splunk indexes
   */
  public EntityCollection<Index> getIndexes() {
    return service.getIndexes();
  }
}
