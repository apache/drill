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
package org.apache.drill.exec.server.rest;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.ThreadDumpServlet;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.rest.auth.DrillRestLoginService;
import org.apache.drill.exec.work.WorkManager;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.security.Authenticator;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.eclipse.jetty.security.authentication.SessionAuthentication;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.session.HashSessionManager;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.glassfish.jersey.servlet.ServletContainer;
import org.joda.time.DateTime;

import javax.servlet.DispatcherType;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.Set;

import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.ADMIN_ROLE;
import static org.apache.drill.exec.server.rest.auth.DrillUserPrincipal.AUTHENTICATED_ROLE;

/**
 * Wrapper class around jetty based webserver.
 */
public class WebServer implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WebServer.class);

  private final DrillConfig config;
  private final MetricRegistry metrics;
  private final WorkManager workManager;
  private final Server embeddedJetty;

  /**
   * Create Jetty based web server.
   * @param config DrillConfig instance.
   * @param metrics Metrics registry.
   * @param workManager WorkManager instance.
   */
  public WebServer(final DrillConfig config, final MetricRegistry metrics, final WorkManager workManager) {
    this.config = config;
    this.metrics = metrics;
    this.workManager = workManager;

    if (config.getBoolean(ExecConstants.HTTP_ENABLE)) {
      embeddedJetty = new Server();
    } else {
      embeddedJetty = null;
    }
  }

  private static final String BASE_STATIC_PATH = "/rest/static/";
  private static final String DRILL_ICON_RESOURCE_RELATIVE_PATH = "img/drill.ico";

  /**
   * Start the web server including setup.
   * @throws Exception
   */
  public void start() throws Exception {
    if (embeddedJetty == null) {
      return;
    }

    final ServerConnector serverConnector;
    if (config.getBoolean(ExecConstants.HTTP_ENABLE_SSL)) {
      serverConnector = createHttpsConnector();
    } else {
      serverConnector = createHttpConnector();
    }
    embeddedJetty.addConnector(serverConnector);

    // Add resources
    final ErrorHandler errorHandler = new ErrorHandler();
    errorHandler.setShowStacks(true);
    errorHandler.setShowMessageInTitle(true);

    final ServletContextHandler servletContextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
    servletContextHandler.setErrorHandler(errorHandler);
    servletContextHandler.setContextPath("/");
    embeddedJetty.setHandler(servletContextHandler);

    final ServletHolder servletHolder = new ServletHolder(new ServletContainer(new DrillRestServer(workManager)));
    servletHolder.setInitOrder(1);
    servletContextHandler.addServlet(servletHolder, "/*");

    servletContextHandler.addServlet(
        new ServletHolder(new MetricsServlet(metrics)), "/status/metrics");
    servletContextHandler.addServlet(new ServletHolder(new ThreadDumpServlet()), "/status/threads");

    final ServletHolder staticHolder = new ServletHolder("static", DefaultServlet.class);
    // Get resource URL for Drill static assets, based on where Drill icon is located
    String drillIconResourcePath =
        Resource.newClassPathResource(BASE_STATIC_PATH + DRILL_ICON_RESOURCE_RELATIVE_PATH).getURL().toString();
    staticHolder.setInitParameter(
        "resourceBase",
        drillIconResourcePath.substring(0,  drillIconResourcePath.length() - DRILL_ICON_RESOURCE_RELATIVE_PATH.length()));
    staticHolder.setInitParameter("dirAllowed", "false");
    staticHolder.setInitParameter("pathInfoOnly", "true");
    servletContextHandler.addServlet(staticHolder, "/static/*");

    if (config.getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED)) {
      servletContextHandler.setSecurityHandler(createSecurityHandler());
      servletContextHandler.setSessionHandler(createSessionHandler(servletContextHandler.getSecurityHandler()));
    }

    if (config.getBoolean(ExecConstants.HTTP_CORS_ENABLED)) {
      FilterHolder holder = new FilterHolder(CrossOriginFilter.class);
      holder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM,
              StringUtils.join(config.getStringList(ExecConstants.HTTP_CORS_ALLOWED_ORIGINS), ","));
      holder.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM,
              StringUtils.join(config.getStringList(ExecConstants.HTTP_CORS_ALLOWED_METHODS), ","));
      holder.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM,
              StringUtils.join(config.getStringList(ExecConstants.HTTP_CORS_ALLOWED_HEADERS), ","));
      holder.setInitParameter(CrossOriginFilter.ALLOW_CREDENTIALS_PARAM,
              String.valueOf(config.getBoolean(ExecConstants.HTTP_CORS_CREDENTIALS)));

      for (String path: new String[] { "*.json", "/storage/*/enable/*", "/status*" }) {
        servletContextHandler.addFilter(holder, path, EnumSet.of(DispatcherType.REQUEST));
      }
    }

    embeddedJetty.start();
  }

  /**
   * @return A {@link SessionHandler} which contains a {@link HashSessionManager}
   */
  private SessionHandler createSessionHandler(final SecurityHandler securityHandler) {
    SessionManager sessionManager = new HashSessionManager();
    sessionManager.setMaxInactiveInterval(config.getInt(ExecConstants.HTTP_SESSION_MAX_IDLE_SECS));
    sessionManager.addEventListener(new HttpSessionListener() {
      @Override
      public void sessionCreated(HttpSessionEvent se) {
        // No-op
      }

      @Override
      public void sessionDestroyed(HttpSessionEvent se) {
        final HttpSession session = se.getSession();
        if (session == null) {
          return;
        }

        final Object authCreds = session.getAttribute(SessionAuthentication.__J_AUTHENTICATED);
        if (authCreds != null) {
          final SessionAuthentication sessionAuth = (SessionAuthentication) authCreds;
          securityHandler.logout(sessionAuth);
          session.removeAttribute(SessionAuthentication.__J_AUTHENTICATED);
        }
      }
    });

    return new SessionHandler(sessionManager);
  }

  /**
   * @return {@link SecurityHandler} with appropriate {@link LoginService}, {@link Authenticator} and constraints.
   */
  private ConstraintSecurityHandler createSecurityHandler() {
    ConstraintSecurityHandler security = new ConstraintSecurityHandler();

    Set<String> knownRoles = ImmutableSet.of(AUTHENTICATED_ROLE, ADMIN_ROLE);
    security.setConstraintMappings(Collections.<ConstraintMapping>emptyList(), knownRoles);

    security.setAuthenticator(new FormAuthenticator("/login", "/login", true));
    security.setLoginService(new DrillRestLoginService(workManager.getContext()));

    return security;
  }

  /**
   * Create an HTTPS connector for given jetty server instance. If the admin has specified keystore/truststore settings
   * they will be used else a self-signed certificate is generated and used.
   *
   * @return Initialized {@link ServerConnector} for HTTPS connectios.
   * @throws Exception
   */
  private ServerConnector createHttpsConnector() throws Exception {
    logger.info("Setting up HTTPS connector for web server");

    final SslContextFactory sslContextFactory = new SslContextFactory();

    if (config.hasPath(ExecConstants.HTTP_KEYSTORE_PATH) &&
        !Strings.isNullOrEmpty(config.getString(ExecConstants.HTTP_KEYSTORE_PATH))) {
      logger.info("Using configured SSL settings for web server");
      sslContextFactory.setKeyStorePath(config.getString(ExecConstants.HTTP_KEYSTORE_PATH));
      sslContextFactory.setKeyStorePassword(config.getString(ExecConstants.HTTP_KEYSTORE_PASSWORD));

      // TrustStore and TrustStore password are optional
      if (config.hasPath(ExecConstants.HTTP_TRUSTSTORE_PATH)) {
        sslContextFactory.setTrustStorePath(config.getString(ExecConstants.HTTP_TRUSTSTORE_PATH));
        if (config.hasPath(ExecConstants.HTTP_TRUSTSTORE_PASSWORD)) {
          sslContextFactory.setTrustStorePassword(config.getString(ExecConstants.HTTP_TRUSTSTORE_PASSWORD));
        }
      }
    } else {
      logger.info("Using generated self-signed SSL settings for web server");
      final SecureRandom random = new SecureRandom();

      // Generate a private-public key pair
      final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
      keyPairGenerator.initialize(1024, random);
      final KeyPair keyPair = keyPairGenerator.generateKeyPair();

      final DateTime now = DateTime.now();

      // Create builder for certificate attributes
      final X500NameBuilder nameBuilder =
          new X500NameBuilder(BCStyle.INSTANCE)
              .addRDN(BCStyle.OU, "Apache Drill (auth-generated)")
              .addRDN(BCStyle.O, "Apache Software Foundation (auto-generated)")
              .addRDN(BCStyle.CN, workManager.getContext().getEndpoint().getAddress());

      final Date notBefore = now.minusMinutes(1).toDate();
      final Date notAfter = now.plusYears(5).toDate();
      final BigInteger serialNumber = new BigInteger(128, random);

      // Create a certificate valid for 5years from now.
      final X509v3CertificateBuilder certificateBuilder = new JcaX509v3CertificateBuilder(
          nameBuilder.build(), // attributes
          serialNumber,
          notBefore,
          notAfter,
          nameBuilder.build(),
          keyPair.getPublic());

      // Sign the certificate using the private key
      final ContentSigner contentSigner =
          new JcaContentSignerBuilder("SHA256WithRSAEncryption").build(keyPair.getPrivate());
      final X509Certificate certificate =
          new JcaX509CertificateConverter().getCertificate(certificateBuilder.build(contentSigner));

      // Check the validity
      certificate.checkValidity(now.toDate());

      // Make sure the certificate is self-signed.
      certificate.verify(certificate.getPublicKey());

      // Generate a random password for keystore protection
      final String keyStorePasswd = RandomStringUtils.random(20);
      final KeyStore keyStore = KeyStore.getInstance("JKS");
      keyStore.load(null, null);
      keyStore.setKeyEntry("DrillAutoGeneratedCert", keyPair.getPrivate(),
          keyStorePasswd.toCharArray(), new java.security.cert.Certificate[]{certificate});

      sslContextFactory.setKeyStore(keyStore);
      sslContextFactory.setKeyStorePassword(keyStorePasswd);
    }

    final HttpConfiguration httpsConfig = new HttpConfiguration();
    httpsConfig.addCustomizer(new SecureRequestCustomizer());

    // SSL Connector
    final ServerConnector sslConnector = new ServerConnector(embeddedJetty,
        new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
        new HttpConnectionFactory(httpsConfig));
    sslConnector.setPort(config.getInt(ExecConstants.HTTP_PORT));

    return sslConnector;
  }

  /**
   * Create HTTP connector.
   * @return Initialized {@link ServerConnector} instance for HTTP connections.
   * @throws Exception
   */
  private ServerConnector createHttpConnector() throws Exception {
    logger.info("Setting up HTTP connector for web server");
    final HttpConfiguration httpConfig = new HttpConfiguration();
    final ServerConnector httpConnector = new ServerConnector(embeddedJetty, new HttpConnectionFactory(httpConfig));
    httpConnector.setPort(config.getInt(ExecConstants.HTTP_PORT));

    return httpConnector;
  }

  @Override
  public void close() throws Exception {
    if (embeddedJetty != null) {
      embeddedJetty.stop();
    }
  }
}
