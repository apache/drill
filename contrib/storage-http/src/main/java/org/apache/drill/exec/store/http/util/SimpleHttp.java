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
package org.apache.drill.exec.store.http.util;

import okhttp3.Authenticator;
import okhttp3.Cache;
import okhttp3.Credentials;
import okhttp3.FormBody;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import okhttp3.Response;

import okhttp3.Route;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.http.HttpAPIConfig;
import org.apache.drill.exec.store.http.HttpAPIConfig.HttpMethods;
import org.apache.drill.exec.store.http.HttpStoragePluginConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


/**
 * Performs the actual HTTP requests for the HTTP Storage Plugin. The core
 * method is the getInputStream() method which accepts a url and opens an
 * InputStream with that URL's contents.
 */
public class SimpleHttp {
  private static final Logger logger = LoggerFactory.getLogger(SimpleHttp.class);

  private final OkHttpClient client;
  private final HttpStoragePluginConfig config;
  private final HttpAPIConfig apiConfig;
  private final File tempDir;
  private final HttpProxyConfig proxyConfig;
  private final CustomErrorContext errorContext;

  public SimpleHttp(HttpStoragePluginConfig config, File tempDir,
      String connectionName, HttpProxyConfig proxyConfig,
      CustomErrorContext errorContext) {
    this.config = config;
    this.tempDir = tempDir;
    this.apiConfig = config.connections().get(connectionName);
    this.proxyConfig = proxyConfig;
    this.errorContext = errorContext;
    this.client = setupHttpClient();
  }

  public InputStream getInputStream(String urlStr) {
    Request.Builder requestBuilder;

    requestBuilder = new Request.Builder()
        .url(urlStr);

    // The configuration does not allow for any other request types other than POST and GET.
    if (apiConfig.getMethodType() == HttpMethods.POST) {
      // Handle POST requests
      FormBody.Builder formBodyBuilder = buildPostBody();
      requestBuilder.post(formBodyBuilder.build());
    }

    // Add headers to request
    if (apiConfig.headers() != null) {
      for (Map.Entry<String, String> entry : apiConfig.headers().entrySet()) {
        requestBuilder.addHeader(entry.getKey(), entry.getValue());
      }
    }

    // Build the request object
    Request request = requestBuilder.build();

    try {
      // Execute the request
      Response response = client
        .newCall(request)
        .execute();

      // If the request is unsuccessful, throw a UserException
      if (!response.isSuccessful()) {
        throw UserException
          .dataReadError()
          .message("Error retrieving data from HTTP Storage Plugin: %d %s",
              response.code(), response.message())
          .addContext(errorContext)
          .build(logger);
      }
      logger.debug("HTTP Request for {} successful.", urlStr);
      logger.debug("Response Headers: {} ", response.headers().toString());

      // Return the InputStream of the response
      return Objects.requireNonNull(response.body()).byteStream();
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message("Error retrieving data from HTTP Storage Plugin: %s", e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }
  }

  /**
   * Configures the OkHTTP3 server object with configuration info from the user.
   *
   * @return OkHttpClient configured server
   */
  private OkHttpClient setupHttpClient() {
    Builder builder = new OkHttpClient.Builder();

    // Set up the HTTP Cache.   Future possibilities include making the cache size and retention configurable but
    // right now it is on or off.  The writer will write to the Drill temp directory if it is accessible and
    // output a warning if not.
    if (config.cacheResults()) {
      setupCache(builder);
    }

    // If the API uses basic authentication add the authentication code.
    if (apiConfig.authType().toLowerCase().equals("basic")) {
      logger.debug("Adding Interceptor");
      builder.addInterceptor(new BasicAuthInterceptor(apiConfig.userName(), apiConfig.password()));
    }

    // Set timeouts
    builder.connectTimeout(config.timeout(), TimeUnit.SECONDS);
    builder.writeTimeout(config.timeout(), TimeUnit.SECONDS);
    builder.readTimeout(config.timeout(), TimeUnit.SECONDS);

    // Set the proxy configuration

    Proxy.Type proxyType;
    switch (proxyConfig.type) {
      case SOCKS:
        proxyType = Proxy.Type.SOCKS;
        break;
      case HTTP:
        proxyType = Proxy.Type.HTTP;
        break;
      default:
        proxyType = Proxy.Type.DIRECT;
    }
    if (proxyType != Proxy.Type.DIRECT) {
      builder.proxy(new Proxy(proxyType,
          new InetSocketAddress(proxyConfig.host, proxyConfig.port)));
      if (proxyConfig.username != null) {
        builder.proxyAuthenticator(new Authenticator() {
          @Override public Request authenticate(Route route, Response response) {
            String credential = Credentials.basic(proxyConfig.username, proxyConfig.password);
            return response.request().newBuilder()
              .header("Proxy-Authorization", credential)
              .build();
          }
        });
      }
    }

    return builder.build();
  }

  /**
   * Configures response caching using a provided temp directory.
   *
   * @param builder
   *          Builder the Builder object to which the caching is to be
   *          configured
   */
  private void setupCache(Builder builder) {
    int cacheSize = 10 * 1024 * 1024;   // TODO Add cache size in MB to config
    File cacheDirectory = new File(tempDir, "http-cache");
    if (!cacheDirectory.mkdirs()) {
      throw UserException.dataWriteError()
        .message("Could not create the HTTP cache directory")
        .addContext("Path", cacheDirectory.getAbsolutePath())
        .addContext("Please check the temp directory or disable HTTP caching.")
        .addContext(errorContext)
        .build(logger);
    }
    try {
      Cache cache = new Cache(cacheDirectory, cacheSize);
      logger.debug("Caching HTTP Query Results at: {}", cacheDirectory);
      builder.cache(cache);
    } catch (Exception e) {
      throw UserException.dataWriteError(e)
        .message("Could not create the HTTP cache")
        .addContext("Path", cacheDirectory.getAbsolutePath())
        .addContext("Please check the temp directory or disable HTTP caching.")
        .addContext(errorContext)
        .build(logger);
    }
  }

  /**
   * Accepts text from a post body in the format:<br>
   * {@code key1=value1}<br>
   * {@code key2=value2}
   * <p>
   * and creates the appropriate headers.
   *
   * @return FormBody.Builder The populated formbody builder
   */
  private FormBody.Builder buildPostBody() {
    final Pattern postBodyPattern = Pattern.compile("^.+=.+$");

    FormBody.Builder formBodyBuilder = new FormBody.Builder();
    String[] lines = apiConfig.postBody().split("\\r?\\n");
    for(String line : lines) {

      // If the string is in the format key=value split it,
      // Otherwise ignore
      if (postBodyPattern.matcher(line).find()) {
        //Split into key/value
        String[] parts = line.split("=");
        formBodyBuilder.add(parts[0], parts[1]);
      }
    }
    return formBodyBuilder;
  }

  /**
   * Intercepts requests and adds authentication headers to the request
   */
  public static class BasicAuthInterceptor implements Interceptor {
    private final String credentials;

    public BasicAuthInterceptor(String user, String password) {
      credentials = Credentials.basic(user, password);
    }

    @NotNull
    @Override
    public Response intercept(Chain chain) throws IOException {
      // Get the existing request
      Request request = chain.request();

      // Replace with new request containing the authorization headers and previous headers
      Request authenticatedRequest = request.newBuilder().header("Authorization", credentials).build();
      return chain.proceed(authenticatedRequest);
    }
  }
}
