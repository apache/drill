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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Proxy;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Factory for creating OkHttpClient instances with enterprise configuration.
 * Centralizes HTTP client setup including proxies, SSL/TLS, custom headers, and timeouts.
 */
public class HttpClientFactory {
  private static final Logger logger = LoggerFactory.getLogger(HttpClientFactory.class);

  private static final Pattern CERT_PATTERN = Pattern.compile(
      "-----BEGIN CERTIFICATE-----(.+?)-----END CERTIFICATE-----", Pattern.DOTALL);
  private static final Pattern KEY_PATTERN = Pattern.compile(
      "-----BEGIN PRIVATE KEY-----(.+?)-----END PRIVATE KEY-----", Pattern.DOTALL);

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

    // 2. Configure proxy. When none is configured, connect directly rather than letting
    // OkHttp inherit the JVM's global -Dhttps.proxyHost: the AI endpoint is often reachable
    // directly (as from a Python client that ignores Java system properties) while a global
    // proxy would silently route — and sometimes reset — the connection. Set the Proxy URL in
    // the Prospector network config if the AI endpoint must go through a proxy.
    if (isSet(config.getProxyUrl())) {
      configureProxy(builder, config);
    } else {
      builder.proxy(Proxy.NO_PROXY);
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

  /** A config path is "set" only if non-null and not blank once trimmed. */
  private static boolean isSet(String value) {
    return value != null && !value.trim().isEmpty();
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
    boolean hasKeystore = isSet(config.getKeystorePath());
    boolean hasTruststore = isSet(config.getTruststorePath());
    boolean hasPemCert = isSet(config.getClientCertPath());

    if (hasKeystore || hasTruststore || hasPemCert) {
      SSLContext sslContext = SSLContext.getInstance("TLS");

      KeyManager[] keyManagers = null;
      if (hasKeystore) {
        keyManagers = loadKeyManagers(config);
      } else if (hasPemCert) {
        keyManagers = loadKeyManagersFromPem(config);
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
   * Load trust managers for custom CA verification. Accepts either a keystore
   * (JKS/PKCS12) or a PEM CA bundle (the {@code verify=} form that Python
   * requests/httpx use — one or more {@code BEGIN CERTIFICATE} blocks, no key).
   * The format is auto-detected from the file contents, so the truststore type
   * does not have to be set for PEM.
   */
  private static TrustManager[] loadTrustManagers(LlmConfig config) throws Exception {
    String truststorePath = config.getTruststorePath().trim();
    byte[] bytes = Files.readAllBytes(Paths.get(truststorePath));

    KeyStore trustStore;
    if (new String(bytes, StandardCharsets.UTF_8).contains("-----BEGIN CERTIFICATE-----")) {
      trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
      trustStore.load(null, null);
      CertificateFactory cf = CertificateFactory.getInstance("X.509");
      Matcher matcher = CERT_PATTERN.matcher(new String(bytes, StandardCharsets.UTF_8));
      int count = 0;
      while (matcher.find()) {
        byte[] der = Base64.getMimeDecoder().decode(matcher.group(1).trim());
        Certificate cert = cf.generateCertificate(new ByteArrayInputStream(der));
        trustStore.setCertificateEntry("ca-" + count++, cert);
      }
      if (count == 0) {
        throw new IllegalArgumentException("No CERTIFICATE block found in truststore PEM: "
            + truststorePath);
      }
    } else {
      String truststorePassword = config.getTruststorePassword();
      String truststoreType = config.getTruststoreType() != null ? config.getTruststoreType() : "JKS";
      trustStore = KeyStore.getInstance(truststoreType);
      try (ByteArrayInputStream in = new ByteArrayInputStream(bytes)) {
        trustStore.load(in, truststorePassword != null ? truststorePassword.toCharArray() : null);
      }
    }

    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);

    return tmf.getTrustManagers();
  }

  /**
   * Load key managers from a PEM file holding the client certificate chain plus an
   * unencrypted PKCS#8 private key (the single-file form that Python httpx/requests
   * accepts as {@code cert=}). Used for mTLS against enterprise gateways.
   *
   * <p>PKCS#1 ({@code BEGIN RSA PRIVATE KEY}) and SEC1 ({@code BEGIN EC PRIVATE KEY}) keys
   * are not supported by the JDK directly; the error message tells the user how to convert.
   */
  private static KeyManager[] loadKeyManagersFromPem(LlmConfig config) throws Exception {
    // Trim so a stray leading/trailing space in the configured path doesn't cause a
    // spurious NoSuchFileException.
    String pemPath = config.getClientCertPath().trim();
    String pem = new String(Files.readAllBytes(Paths.get(pemPath)), StandardCharsets.UTF_8);

    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    List<Certificate> chain = new ArrayList<>();
    Matcher certMatcher = CERT_PATTERN.matcher(pem);
    while (certMatcher.find()) {
      byte[] der = Base64.getMimeDecoder().decode(certMatcher.group(1).trim());
      chain.add(cf.generateCertificate(new ByteArrayInputStream(der)));
    }
    if (chain.isEmpty()) {
      throw new IllegalArgumentException("No CERTIFICATE block found in PEM: " + pemPath);
    }

    Matcher keyMatcher = KEY_PATTERN.matcher(pem);
    if (!keyMatcher.find()) {
      if (pem.contains("BEGIN RSA PRIVATE KEY") || pem.contains("BEGIN EC PRIVATE KEY")) {
        throw new IllegalArgumentException("PEM private key is PKCS#1/SEC1, which the JDK "
            + "cannot read. Convert to PKCS#8 with: openssl pkcs8 -topk8 -nocrypt -in "
            + pemPath + " -out client-pkcs8.pem");
      }
      throw new IllegalArgumentException("No unencrypted PRIVATE KEY (PKCS#8) block found in PEM: "
          + pemPath);
    }
    byte[] keyDer = Base64.getMimeDecoder().decode(keyMatcher.group(1).trim());
    PrivateKey privateKey = parsePkcs8PrivateKey(keyDer);

    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, null);
    keyStore.setKeyEntry("client", privateKey, new char[0], chain.toArray(new Certificate[0]));

    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, new char[0]);
    return kmf.getKeyManagers();
  }

  /** The PKCS#8 blob carries its own algorithm OID, so try the common ones until one parses. */
  private static PrivateKey parsePkcs8PrivateKey(byte[] der) throws Exception {
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
    for (String algorithm : new String[] {"RSA", "EC"}) {
      try {
        return KeyFactory.getInstance(algorithm).generatePrivate(spec);
      } catch (Exception ignored) {
        // Wrong algorithm for this key — try the next.
      }
    }
    throw new IllegalArgumentException("Unsupported client-cert private key algorithm "
        + "(expected RSA or EC PKCS#8)");
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
