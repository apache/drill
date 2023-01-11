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

import com.google.api.client.util.SslUtils;
import org.apache.drill.categories.SlowTest;
import org.apache.drill.test.BaseTest;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

@Category(SlowTest.class)
@RunWith(Suite.class)
@Suite.SuiteClasses({ElasticComplexTypesTest.class,
    ElasticInfoSchemaTest.class,
    ElasticSearchPlanTest.class,
    ElasticSearchQueryTest.class,
    ElasticSearchUserTranslationTest.class
})
public class TestElasticsearchSuite extends BaseTest {

  public static final String ELASTICSEARCH_USERNAME = "elastic";
  public static final String ELASTICSEARCH_PASSWORD = "password";

  protected static ElasticsearchContainer elasticsearch;
  private static RestClient restClient;


  private static final AtomicInteger initCount = new AtomicInteger(0);

  private static volatile boolean runningSuite = false;

  @BeforeClass
  public static void initElasticsearch() {
    synchronized (TestElasticsearchSuite.class) {
      if (initCount.get() == 0) {
        startElasticsearch();
      }
      initCount.incrementAndGet();
      runningSuite = true;
    }
  }

  public static boolean isRunningSuite() {
    return runningSuite;
  }

  @AfterClass
  public static void tearDownCluster() {
    synchronized (TestElasticsearchSuite.class) {
      if (initCount.decrementAndGet() == 0 && elasticsearch != null) {
        elasticsearch.stop();
      }
    }
  }

  public static CredentialsProvider getEsCredentialsProvider() {
    CredentialsProvider esCredentialsProvider = new BasicCredentialsProvider();
    esCredentialsProvider.setCredentials(
        AuthScope.ANY,
        new UsernamePasswordCredentials(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)
    );
    return esCredentialsProvider;
  }

  public static RestHighLevelClient getClient() {

    return new RestHighLevelClient(
        RestClient
            .builder(HttpHost.create(TestElasticsearchSuite.getAddress()))
            .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(getEsCredentialsProvider()))
    );
  }

  private static void startElasticsearch() {
    DockerImageName imageName = DockerImageName.parse("elasticsearch:8.0.0")
      .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch");
    TestElasticsearchSuite.elasticsearch = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.0.0")
        .withExposedPorts(9200)
        .withPassword("s3cret");
    // HttpsURLConnection.setDefaultSSLSocketFactory(SslUtils.trustAllContext().getSocketFactory());
    TestElasticsearchSuite.elasticsearch.getEnvMap().remove("xpack.security.enabled");

    TestElasticsearchSuite.elasticsearch.setWaitStrategy(new HttpWaitStrategy()
        .forPort(9200)
        .usingTls()
        .forStatusCode(HTTP_OK)
        .forStatusCode(HTTP_UNAUTHORIZED)
        .withBasicCredentials("elastic", "secr3t")
        .withStartupTimeout(Duration.ofMinutes(2)));

    TestElasticsearchSuite.elasticsearch.start();

    // extract the ca cert from the running instance
    byte[] certAsBytes = elasticsearch.copyFileFromContainer("/usr/share/elasticsearch/config/certs/http_ca.crt",
        TestElasticsearchSuite::apply);
    HttpHost host = new HttpHost("localhost", elasticsearch.getMappedPort(9200), "https");

    RestClientBuilder builder = RestClient.builder(host);

    builder.setHttpClientConfigCallback(clientBuilder -> {
      try {
        clientBuilder.setSSLContext(SslUtils.trustAllSSLContext());
      } catch (GeneralSecurityException e) {
        throw new RuntimeException(e);
      }
      clientBuilder.setDefaultCredentialsProvider(getEsCredentialsProvider());
      return clientBuilder;
    });
  }

  public static String getAddress() {
    return elasticsearch.getHttpHostAddress();
  }

  private static byte[] apply(InputStream inputStream) {
    final byte[] bytes;
    bytes = inputStream.readAllBytes();
    return bytes;
  }
}
