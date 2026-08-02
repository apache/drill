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
package org.apache.drill.exec.store.accumulo;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.DelegationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/**
 * Manages Accumulo client connections with support for multiple authentication modes.
 *
 * <p>This class centralizes all Accumulo client creation and authentication logic,
 * supporting:</p>
 * <ul>
 *   <li>Password authentication (username/password)</li>
 *   <li>Kerberos authentication (principal/keytab)</li>
 *   <li>User translation (per-user credentials lookup)</li>
 *   <li>Delegation tokens for user impersonation in distributed execution</li>
 * </ul>
 *
 * <h3>Authentication Modes:</h3>
 * <ul>
 *   <li><b>SHARED_USER:</b> All queries use a single shared service client</li>
 *   <li><b>USER_TRANSLATION:</b> Per-user Accumulo credentials are looked up from
 *       the CredentialsProvider based on the Drill query user</li>
 *   <li><b>USER_IMPERSONATION:</b> Service authenticates with Kerberos, then
 *       impersonates the query user via delegation tokens</li>
 * </ul>
 */
public class AccumuloConnectionManager implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(AccumuloConnectionManager.class);

  /**
   * Default TTL for cached delegation tokens (1 hour).
   * Tokens older than this will be refreshed.
   */
  private static final long DEFAULT_TOKEN_TTL_MILLIS = TimeUnit.HOURS.toMillis(1);

  /**
   * Default delegation token lifetime when requesting from Accumulo.
   */
  private static final long DEFAULT_TOKEN_LIFETIME_MILLIS = TimeUnit.HOURS.toMillis(24);

  private final AccumuloStoragePluginConfig config;

  /**
   * Shared service client (for PASSWORD or KERBEROS + SHARED_USER mode).
   */
  private volatile AccumuloClient serviceClient;
  private final Object serviceClientLock = new Object();

  /**
   * Cache of per-user clients for USER_TRANSLATION mode.
   * Key is the Accumulo username (from translated credentials).
   */
  private final Map<String, AccumuloClient> userClientCache = new ConcurrentHashMap<>();

  /**
   * Cache of delegation tokens per user for USER_IMPERSONATION mode.
   */
  private final Map<String, DelegationTokenInfo> delegationTokenCache = new ConcurrentHashMap<>();

  public AccumuloConnectionManager(AccumuloStoragePluginConfig config) {
    this.config = config;
  }

  /**
   * Returns the shared service client.
   *
   * <p>For PASSWORD mode, this uses the configured username/password.
   * For KERBEROS mode, this authenticates using the service principal and keytab.</p>
   *
   * <p>The client is lazily initialized and cached for reuse.</p>
   *
   * @return the service AccumuloClient
   * @throws UserException if connection fails
   */
  public AccumuloClient getServiceClient() {
    if (serviceClient == null) {
      synchronized (serviceClientLock) {
        if (serviceClient == null) {
          try {
            serviceClient = createServiceClient();
            logger.info("Created Accumulo service client for instance: {}", config.getInstanceName());
          } catch (AccumuloException | AccumuloSecurityException | IOException e) {
            throw UserException.connectionError(e)
                .message("Failed to connect to Accumulo instance '%s' at '%s'",
                    config.getInstanceName(), config.getZookeeperQuorum())
                .addContext("AuthenticationType", config.getAuthenticationType())
                .build(logger);
          }
        }
      }
    }
    return serviceClient;
  }

  /**
   * Returns an AccumuloClient for the specified user.
   *
   * <p>Behavior depends on the auth mode:</p>
   * <ul>
   *   <li>SHARED_USER: Returns the service client (same for all users)</li>
   *   <li>USER_TRANSLATION: Creates/returns a client using per-user credentials</li>
   *   <li>USER_IMPERSONATION: Returns a client using the user's delegation token</li>
   * </ul>
   *
   * @param userName the Drill query user name
   * @return an AccumuloClient for the user
   * @throws UserException if client creation fails
   */
  public AccumuloClient getClientForUser(String userName) {
    return getClientForUser(userName, null);
  }

  /**
   * Returns an AccumuloClient for the specified user with optional user credentials.
   *
   * @param userName the Drill query user name
   * @param userCredentials the user credentials (for USER_TRANSLATION mode)
   * @return an AccumuloClient for the user
   * @throws UserException if client creation fails
   */
  public AccumuloClient getClientForUser(String userName, UserCredentials userCredentials) {
    AuthMode authMode = config.getAuthMode();

    switch (authMode) {
      case SHARED_USER:
        return getServiceClient();

      case USER_TRANSLATION:
        return getClientForUserTranslation(userName, userCredentials);

      case USER_IMPERSONATION:
        return getClientForUserImpersonation(userName);

      default:
        throw UserException.connectionError()
            .message("Unsupported auth mode: %s", authMode)
            .build(logger);
    }
  }

  /**
   * Creates or returns a cached client for USER_TRANSLATION mode.
   *
   * <p>In this mode, per-user Accumulo credentials are looked up from the
   * CredentialsProvider based on the Drill query user.</p>
   */
  private AccumuloClient getClientForUserTranslation(String userName, UserCredentials userCredentials) {
    // Build UserCredentials if not provided
    if (userCredentials == null && userName != null) {
      userCredentials = UserCredentials.newBuilder()
          .setUserName(userName)
          .build();
    }

    // Look up per-user credentials
    Optional<UsernamePasswordCredentials> creds = config.getUsernamePasswordCredentials(userCredentials);

    if (!creds.isPresent()) {
      throw UserException.connectionError()
          .message("No credentials found for user '%s' in USER_TRANSLATION mode. " +
              "Please configure credentials for this user in the storage plugin.", userName)
          .addContext("Plugin", "accumulo")
          .addContext("AuthMode", "USER_TRANSLATION")
          .build(logger);
    }

    String accumuloUsername = creds.get().getUsername();
    String accumuloPassword = creds.get().getPassword();

    // Check cache first (keyed by Accumulo username)
    AccumuloClient cachedClient = userClientCache.get(accumuloUsername);
    if (cachedClient != null) {
      logger.debug("Using cached client for translated user: {} -> {}", userName, accumuloUsername);
      return cachedClient;
    }

    // Create new client
    synchronized (userClientCache) {
      // Double-check
      cachedClient = userClientCache.get(accumuloUsername);
      if (cachedClient != null) {
        return cachedClient;
      }

      try {
        logger.info("Creating Accumulo client for translated user: {} -> {}", userName, accumuloUsername);

        Properties props = new Properties();
        props.setProperty("instance.name", config.getInstanceName());
        props.setProperty("instance.zookeepers", config.getZookeeperQuorum());

        AccumuloClient client = Accumulo.newClient()
            .from(props)
            .as(accumuloUsername, new PasswordToken(accumuloPassword))
            .build();

        userClientCache.put(accumuloUsername, client);
        return client;

      } catch (Exception e) {
        throw UserException.connectionError(e)
            .message("Failed to create Accumulo client for translated user '%s' (Accumulo user: '%s')",
                userName, accumuloUsername)
            .build(logger);
      }
    }
  }

  /**
   * Creates a client for USER_IMPERSONATION mode using delegation tokens.
   */
  private AccumuloClient getClientForUserImpersonation(String userName) {
    if (config.getAuthenticationType() != AccumuloAuthType.KERBEROS) {
      throw UserException.connectionError()
          .message("User impersonation requires Kerberos authentication")
          .build(logger);
    }

    if (!config.isUseDelegationTokens()) {
      throw UserException.connectionError()
          .message("User impersonation requires delegation tokens to be enabled")
          .build(logger);
    }

    try {
      DelegationTokenInfo tokenInfo = getDelegationToken(userName);
      return createClientWithDelegationToken(tokenInfo);
    } catch (Exception e) {
      throw UserException.connectionError(e)
          .message("Failed to create impersonated client for user '%s'", userName)
          .build(logger);
    }
  }

  /**
   * Gets or creates a delegation token for the specified user.
   *
   * <p>Tokens are cached with a TTL to avoid repeated token creation.
   * This method is thread-safe.</p>
   *
   * @param userName the user to get a delegation token for
   * @return the delegation token info
   * @throws AccumuloException if token creation fails
   * @throws AccumuloSecurityException if authentication fails
   * @throws IOException if token serialization fails
   */
  public DelegationTokenInfo getDelegationToken(String userName)
      throws AccumuloException, AccumuloSecurityException, IOException {

    // Check cache first
    DelegationTokenInfo cached = delegationTokenCache.get(userName);
    if (cached != null && !cached.isOlderThan(DEFAULT_TOKEN_TTL_MILLIS)) {
      logger.debug("Using cached delegation token for user: {}", userName);
      return cached;
    }

    // Need to create/refresh token
    synchronized (delegationTokenCache) {
      // Double-check after acquiring lock
      cached = delegationTokenCache.get(userName);
      if (cached != null && !cached.isOlderThan(DEFAULT_TOKEN_TTL_MILLIS)) {
        return cached;
      }

      logger.info("Creating delegation token for user: {}", userName);

      // Use the service client to obtain a delegation token
      AccumuloClient client = getServiceClient();

      // Create a proxy user UGI for the query user
      UserGroupInformation proxyUgi = ImpersonationUtil.createProxyUgi(userName);

      // Request delegation token for the proxy user
      DelegationTokenConfig tokenConfig = new DelegationTokenConfig();
      tokenConfig.setTokenLifetime(DEFAULT_TOKEN_LIFETIME_MILLIS, TimeUnit.MILLISECONDS);

      DelegationToken token = client.securityOperations()
          .getDelegationToken(tokenConfig);

      DelegationTokenInfo tokenInfo = DelegationTokenInfo.fromDelegationToken(userName, token);
      delegationTokenCache.put(userName, tokenInfo);

      logger.info("Created delegation token for user: {} (expires in {} ms)",
          userName, DEFAULT_TOKEN_LIFETIME_MILLIS);

      return tokenInfo;
    }
  }

  /**
   * Creates an AccumuloClient using a delegation token.
   *
   * <p>This method is used by distributed fragments to create a client
   * with the delegated user identity.</p>
   *
   * @param tokenInfo the delegation token info
   * @return a new AccumuloClient authenticated with the delegation token
   * @throws AccumuloException if client creation fails
   * @throws AccumuloSecurityException if authentication fails
   */
  public AccumuloClient createClientWithDelegationToken(DelegationTokenInfo tokenInfo)
      throws AccumuloException, AccumuloSecurityException {

    AuthenticationToken token = tokenInfo.toAuthenticationToken();

    Properties props = new Properties();
    props.setProperty("instance.name", config.getInstanceName());
    props.setProperty("instance.zookeepers", config.getZookeeperQuorum());

    // Configure SASL for delegation token authentication
    if (!Strings.isNullOrEmpty(config.getSaslQop())) {
      props.setProperty("rpc.sasl.qop", config.getSaslQop());
    }

    return Accumulo.newClient()
        .from(props)
        .as(tokenInfo.getUserName(), token)
        .build();
  }

  /**
   * Creates an AccumuloClient using username/password credentials.
   *
   * <p>This method is used for USER_TRANSLATION mode where per-user
   * credentials are stored in the CredentialsProvider.</p>
   *
   * @param credentials the username/password credentials
   * @return a new AccumuloClient
   * @throws AccumuloException if client creation fails
   * @throws AccumuloSecurityException if authentication fails
   */
  public AccumuloClient createClientWithCredentials(UsernamePasswordCredentials credentials)
      throws AccumuloException, AccumuloSecurityException {

    Properties props = new Properties();
    props.setProperty("instance.name", config.getInstanceName());
    props.setProperty("instance.zookeepers", config.getZookeeperQuorum());

    return Accumulo.newClient()
        .from(props)
        .as(credentials.getUsername(), new PasswordToken(credentials.getPassword()))
        .build();
  }

  /**
   * Creates the service client based on the configured authentication type.
   */
  private AccumuloClient createServiceClient()
      throws AccumuloException, AccumuloSecurityException, IOException {

    AccumuloAuthType authType = config.getAuthenticationType();

    Properties props = new Properties();
    props.setProperty("instance.name", config.getInstanceName());
    props.setProperty("instance.zookeepers", config.getZookeeperQuorum());

    String principal;
    AuthenticationToken token;

    if (authType == AccumuloAuthType.KERBEROS) {
      principal = config.getPrincipal();
      token = createKerberosToken();

      // Configure SASL properties
      if (!Strings.isNullOrEmpty(config.getSaslQop())) {
        props.setProperty("rpc.sasl.qop", config.getSaslQop());
      }
      if (!Strings.isNullOrEmpty(config.getAccumuloServicePrimary())) {
        props.setProperty("sasl.kerberos.server.primary", config.getAccumuloServicePrimary());
      }
    } else {
      // PASSWORD authentication
      Optional<UsernamePasswordCredentials> creds = config.getUsernamePasswordCredentials(null);
      if (creds.isPresent()) {
        principal = creds.get().getUsername();
        token = new PasswordToken(creds.get().getPassword());
      } else {
        principal = config.getUsername();
        token = new PasswordToken(config.getPassword());
      }
    }

    logger.debug("Creating Accumulo client with auth type: {}, principal: {}", authType, principal);

    return Accumulo.newClient()
        .from(props)
        .as(principal, token)
        .build();
  }

  /**
   * Creates a KerberosToken for service authentication.
   */
  private KerberosToken createKerberosToken() throws IOException {
    String keytabPath = config.getKeytabPath();
    String principal = config.getPrincipal();

    if (Strings.isNullOrEmpty(keytabPath)) {
      throw new IOException("Keytab path is required for Kerberos authentication");
    }
    if (Strings.isNullOrEmpty(principal)) {
      throw new IOException("Principal is required for Kerberos authentication");
    }

    File keytabFile = new File(keytabPath);
    if (!keytabFile.exists()) {
      throw new IOException("Keytab file does not exist: " + keytabPath);
    }
    if (!keytabFile.canRead()) {
      throw new IOException("Cannot read keytab file: " + keytabPath);
    }

    logger.info("Logging in with Kerberos principal: {} using keytab: {}", principal, keytabPath);

    // Login using the keytab
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath);

    return new KerberosToken();
  }

  /**
   * Clears the delegation token cache.
   * This forces new tokens to be obtained on next request.
   */
  public void clearDelegationTokenCache() {
    delegationTokenCache.clear();
    logger.debug("Cleared delegation token cache");
  }

  /**
   * Returns the number of cached delegation tokens.
   * Primarily for testing/monitoring purposes.
   */
  public int getDelegationTokenCacheSize() {
    return delegationTokenCache.size();
  }

  /**
   * Returns the number of cached user clients (for USER_TRANSLATION mode).
   * Primarily for testing/monitoring purposes.
   */
  public int getUserClientCacheSize() {
    return userClientCache.size();
  }

  @Override
  public void close() {
    // Close service client
    synchronized (serviceClientLock) {
      if (serviceClient != null) {
        try {
          logger.debug("Closing Accumulo service client for instance: {}", config.getInstanceName());
          serviceClient.close();
        } catch (Exception e) {
          logger.warn("Error closing Accumulo service client", e);
        }
        serviceClient = null;
      }
    }

    // Close all cached user clients
    for (Map.Entry<String, AccumuloClient> entry : userClientCache.entrySet()) {
      try {
        logger.debug("Closing cached client for user: {}", entry.getKey());
        entry.getValue().close();
      } catch (Exception e) {
        logger.warn("Error closing cached client for user: {}", entry.getKey(), e);
      }
    }
    userClientCache.clear();

    // Clear delegation token cache
    clearDelegationTokenCache();
  }
}
