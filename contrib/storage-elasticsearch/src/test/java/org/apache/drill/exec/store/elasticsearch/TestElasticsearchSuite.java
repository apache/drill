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
package org.apache.drill.exec.store.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.commons.io.IOUtils;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.test.BaseTest;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import com.google.api.client.util.SslUtils;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;


@Category(SlowTest.class)
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ElasticComplexTypesTest.class,
    ElasticInfoSchemaTest.class,
    ElasticSearchPlanTest.class,
    ElasticSearchQueryTest.class,
    ElasticSearchUserTranslationTest.class})
public class TestElasticsearchSuite extends BaseTest {

  protected static ElasticsearchContainer elasticsearch;
  public static final String ELASTICSEARCH_USERNAME = "elastic";
  public static final String ELASTICSEARCH_PASSWORD = "s3cret";
  private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:8.9.1";

  private static final AtomicInteger initCount = new AtomicInteger(0);

  private static volatile boolean runningSuite = false;

  @BeforeClass
  public static void initElasticsearch() throws IOException, GeneralSecurityException {
    synchronized (TestElasticsearchSuite.class) {
      if (initCount.get() == 0) {
        startElasticsearch();
      }
      initCount.incrementAndGet();
      runningSuite = true;
    }
  }

  @AfterClass
  public static void tearDownCluster() {
    synchronized (TestElasticsearchSuite.class) {
      if (initCount.decrementAndGet() == 0 && elasticsearch != null) {
        elasticsearch.stop();
        elasticsearch.close();
      }
    }
  }

  public static boolean isRunningSuite() {
    return runningSuite;
  }

  /**
   * Returns an {@link CredentialsProvider} for use with the ElasticSearch test container.
   * @return A {@link CredentialsProvider} with the default credentials.
   */
  public static CredentialsProvider getCredentialsProvider() {
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD));
    return credentialsProvider;
  }

  public static byte[] getCertAsBytes(ElasticsearchContainer container) {
    return container.copyFileFromContainer(
        "/usr/share/elasticsearch/config/certs/http_ca.crt",
        IOUtils::toByteArray);
  }

  public static SSLContext createContextFromCaCert(byte[] certAsBytes) {
    try {
      CertificateFactory factory = CertificateFactory.getInstance("X.509");
      Certificate trustedCa = factory.generateCertificate(
          new ByteArrayInputStream(certAsBytes)
      );
      KeyStore trustStore = KeyStore.getInstance("pkcs12");
      trustStore.load(null, null);
      trustStore.setCertificateEntry("ca", trustedCa);
      SSLContextBuilder sslContextBuilder =
          SSLContexts.custom().loadTrustMaterial(trustStore, null);
      return sslContextBuilder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static ElasticsearchClient getESClient() {
    HttpHost host = new HttpHost(
      elasticsearch.getHost(),
      elasticsearch.getMappedPort(9200),
      "http"
    );

    final RestClientBuilder builder = RestClient.builder(host);

    builder.setHttpClientConfigCallback(clientBuilder -> {
      //clientBuilder.setSSLContext(TestElasticsearchSuite.createContextFromCaCert(getCertAsBytes(elasticsearch)));
      clientBuilder.setDefaultCredentialsProvider(getCredentialsProvider());
      return clientBuilder;
    });

    ElasticsearchTransport transport = new RestClientTransport(
        builder.build(), new JacksonJsonpMapper());

    return new ElasticsearchClient(transport);
  }



  private static void startElasticsearch() throws GeneralSecurityException {
    elasticsearch = new ElasticsearchContainer(IMAGE_NAME)
        .withExposedPorts(9200)
        .withStartupTimeout(Duration.ofMinutes(2))
        .withStartupAttempts(5)
        .withPassword(ELASTICSEARCH_PASSWORD)
        .withEnv("xpack.security.enabled", "true")
        .withEnv("xpack.security.transport.ssl.enabled", "false")
        .withEnv("discovery.type", "single-node")
        .withEnv("ES_JAVA_OPTS", "-Xmx2g"); // ES gobbles up lots of RAM under defaults.

    HttpsURLConnection.setDefaultSSLSocketFactory(SslUtils.trustAllSSLContext().getSocketFactory());
    elasticsearch.start();
  }

  public static String getAddress() {
    return elasticsearch.getHttpHostAddress();
  }
}
