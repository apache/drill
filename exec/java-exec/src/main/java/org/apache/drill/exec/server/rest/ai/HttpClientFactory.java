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
package org.apache.drill.exec.server.rest.ai;

import okhttp3.Credentials;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Proxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;

/**
 * Factory for creating OkHttpClient instances with enterprise configuration.
 * Centralizes HTTP client setup including proxies, SSL/TLS, custom headers, and timeouts.
 */
public class HttpClientFactory {
  private static final Logger logger = LoggerFactory.getLogger(HttpClientFactory.class);

  // Default timeouts (in seconds) - matching existing hardcoded values
  private static final int DEFAULT_CONNECT_TIMEOUT = 30;
  private static final int DEFAULT_READ_TIMEOUT = 120;
  private static final int DEFAULT_WRITE_TIMEOUT = 30;

  /**
   * Create an OkHttpClient configured with all enterprise settings from LlmConfig.
   *
   * @param config The LLM configuration
   * @return Configured OkHttpClient
   * @throws Exception if SSL/TLS configuration or file loading fails
   */
  public static OkHttpClient createClient(LlmConfig config) throws Exception {
    OkHttpClient.Builder builder = new OkHttpClient.Builder();

    // 1. Configure timeouts
    int connectTimeout = config.getConnectTimeoutSeconds() != null
        ? config.getConnectTimeoutSeconds() : DEFAULT_CONNECT_TIMEOUT;
    int readTimeout = config.getReadTimeoutSeconds() != null
        ? config.getReadTimeoutSeconds() : DEFAULT_READ_TIMEOUT;
    int writeTimeout = config.getWriteTimeoutSeconds() != null
        ? config.getWriteTimeoutSeconds() : DEFAULT_WRITE_TIMEOUT;

    builder.connectTimeout(connectTimeout, TimeUnit.SECONDS)
        .readTimeout(readTimeout, TimeUnit.SECONDS)
        .writeTimeout(writeTimeout, TimeUnit.SECONDS);

    // 2. Configure proxy
    if (config.getProxyUrl() != null && !config.getProxyUrl().isEmpty()) {
      configureProxy(builder, config);
    }

    // 3. Configure SSL/TLS
    configureSSL(builder, config);

    // 4. Add custom headers interceptor
    if (config.getCustomHeaders() != null && !config.getCustomHeaders().isEmpty()) {
      builder.addInterceptor(new CustomHeadersInterceptor(config.getCustomHeaders()));
    }

    // 5. Add logging/debugging interceptor (redacts sensitive headers)
    if (logger.isDebugEnabled()) {
      builder.addInterceptor(new SensitiveHeaderRedactor());
    }

    return builder.build();
  }

  /**
   * Configure HTTP proxy with optional authentication.
   */
  private static void configureProxy(OkHttpClient.Builder builder, LlmConfig config) {
    HttpUrl proxyUrl = HttpUrl.parse(config.getProxyUrl());
    if (proxyUrl == null) {
      throw new IllegalArgumentException("Invalid proxy URL: " + config.getProxyUrl());
    }

    Proxy proxy = new Proxy(Proxy.Type.HTTP,
        new InetSocketAddress(proxyUrl.host(), proxyUrl.port()));
    builder.proxy(proxy);

    // Proxy authentication
    if (config.getProxyUsername() != null && !config.getProxyUsername().isEmpty()) {
      builder.proxyAuthenticator((route, response) -> {
        String credential = Credentials.basic(
            config.getProxyUsername(),
            config.getProxyPassword() != null ? config.getProxyPassword() : ""
        );
        return response.request().newBuilder()
            .header("Proxy-Authorization", credential)
            .build();
      });
    }
  }

  /**
   * Configure SSL/TLS with optional custom truststore (CA certificates) and keystore (client certificates).
   */
  private static void configureSSL(OkHttpClient.Builder builder, LlmConfig config)
      throws Exception {
    // Check if custom SSL configuration is needed
    boolean hasKeystore = config.getKeystorePath() != null && !config.getKeystorePath().isEmpty();
    boolean hasTruststore = config.getTruststorePath() != null && !config.getTruststorePath().isEmpty();

    if (hasKeystore || hasTruststore) {
      SSLContext sslContext = SSLContext.getInstance("TLS");

      KeyManager[] keyManagers = null;
      if (hasKeystore) {
        keyManagers = loadKeyManagers(config);
      }

      TrustManager[] trustManagers = null;
      if (hasTruststore) {
        trustManagers = loadTrustManagers(config);
      }

      sslContext.init(keyManagers, trustManagers, new SecureRandom());

      X509TrustManager trustManager = trustManagers != null
          ? (X509TrustManager) trustManagers[0]
          : getDefaultTrustManager();

      builder.sslSocketFactory(sslContext.getSocketFactory(), trustManager);
    }

    // Disable SSL verification if explicitly set to false (INSECURE - log warning)
    if (Boolean.FALSE.equals(config.getVerifySSL())) {
      logger.warn("SSL certificate verification is disabled! Use only for testing/development.");
      builder.hostnameVerifier((hostname, session) -> true);
    }
  }

  /**
   * Load key managers from keystore for mTLS (mutual TLS / client certificate) support.
   */
  private static KeyManager[] loadKeyManagers(LlmConfig config) throws Exception {
    String keystorePath = config.getKeystorePath();
    String keystorePassword = config.getKeystorePassword();
    String keystoreType = config.getKeystoreType() != null ? config.getKeystoreType() : "JKS";

    KeyStore keyStore = KeyStore.getInstance(keystoreType);
    try (FileInputStream fis = new FileInputStream(keystorePath)) {
      keyStore.load(fis, keystorePassword != null ? keystorePassword.toCharArray() : null);
    }

    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, keystorePassword != null ? keystorePassword.toCharArray() : null);

    return kmf.getKeyManagers();
  }

  /**
   * Load trust managers from truststore for custom CA certificate support.
   */
  private static TrustManager[] loadTrustManagers(LlmConfig config) throws Exception {
    String truststorePath = config.getTruststorePath();
    String truststorePassword = config.getTruststorePassword();
    String truststoreType = config.getTruststoreType() != null ? config.getTruststoreType() : "JKS";

    KeyStore trustStore = KeyStore.getInstance(truststoreType);
    try (FileInputStream fis = new FileInputStream(truststorePath)) {
      trustStore.load(fis, truststorePassword != null ? truststorePassword.toCharArray() : null);
    }

    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);

    return tmf.getTrustManagers();
  }

  /**
   * Get the default system trust manager.
   */
  private static X509TrustManager getDefaultTrustManager() throws Exception {
    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init((KeyStore) null);
    for (TrustManager tm : tmf.getTrustManagers()) {
      if (tm instanceof X509TrustManager) {
        return (X509TrustManager) tm;
      }
    }
    throw new IllegalStateException("No X509TrustManager found");
  }
}
