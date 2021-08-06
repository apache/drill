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

import lombok.extern.slf4j.Slf4j;
import okhttp3.Cache;
import okhttp3.Credentials;
import okhttp3.FormBody;
import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import okhttp3.Response;

import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.http.HttpApiConfig;
import org.apache.drill.exec.store.http.HttpApiConfig.HttpMethod;
import org.apache.drill.exec.store.http.HttpStoragePluginConfig;
import org.apache.drill.exec.store.http.HttpSubScan;
import org.apache.drill.exec.store.security.UsernamePasswordCredentials;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Performs the actual HTTP requests for the HTTP Storage Plugin. The core
 * method is the getInputStream() method which accepts a url and opens an
 * InputStream with that URL's contents.
 */
@Slf4j
public class SimpleHttp {
  private static final Logger logger = LoggerFactory.getLogger(SimpleHttp.class);
  private static final Pattern URL_PARAM_REGEX = Pattern.compile("\\{(\\w+)\\}");

  private final OkHttpClient client;
  private final HttpSubScan scanDefn;
  private final File tempDir;
  private final HttpProxyConfig proxyConfig;
  private final CustomErrorContext errorContext;
  private final HttpUrl url;
  private String responseMessage;
  private int responseCode;
  private String responseProtocol;
  private String responseURL;

  @lombok.Builder
  public SimpleHttp(HttpSubScan scanDefn, HttpUrl url, File tempDir,
    HttpProxyConfig proxyConfig, CustomErrorContext errorContext) {
    this.scanDefn = scanDefn;
    this.url = url;
    this.tempDir = tempDir;
    this.proxyConfig = proxyConfig;
    this.errorContext = errorContext;
    this.client = setupHttpClient();
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
    HttpStoragePluginConfig config = scanDefn.tableSpec().config();
    if (config.cacheResults()) {
      setupCache(builder);
    }

    // If the API uses basic authentication add the authentication code.
    HttpApiConfig apiConfig = scanDefn.tableSpec().connectionConfig();
    if (apiConfig.authType().equalsIgnoreCase("basic")) {
      logger.debug("Adding Interceptor");
      UsernamePasswordCredentials credentials = apiConfig.getUsernamePasswordCredentials();
      builder.addInterceptor(new BasicAuthInterceptor(credentials.getUsername(), credentials.getPassword()));
    }

    // Set timeouts
    int timeout = Math.max(1, config.timeout());
    builder.connectTimeout(timeout, TimeUnit.SECONDS);
    builder.writeTimeout(timeout, TimeUnit.SECONDS);
    builder.readTimeout(timeout, TimeUnit.SECONDS);

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
        builder.proxyAuthenticator((route, response) -> {
          String credential = Credentials.basic(proxyConfig.username, proxyConfig.password);
          return response.request().newBuilder()
            .header("Proxy-Authorization", credential)
            .build();
        });
      }
    }

    return builder.build();
  }

  public String url() {
    return url.toString();
  }

  public InputStream getInputStream() {

    Request.Builder requestBuilder = new Request.Builder()
      .url(url);

    // The configuration does not allow for any other request types other than POST and GET.
    HttpApiConfig apiConfig = scanDefn.tableSpec().connectionConfig();
    if (apiConfig.getMethodType() == HttpMethod.POST) {
      // Handle POST requests
      FormBody.Builder formBodyBuilder = buildPostBody(apiConfig.postBody());
      requestBuilder.post(formBodyBuilder.build());
    }

    // Log the URL and method to aid in debugging user issues.
    logger.info("Connection: {}, Method {}, URL: {}",
      scanDefn.tableSpec().connection(),
      apiConfig.getMethodType().name(), url());

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

      // Preserve the response
      responseMessage = response.message();
      responseCode = response.code();
      responseProtocol = response.protocol().toString();
      responseURL = response.request().url().toString();

      // If the request is unsuccessful, throw a UserException
      if (!isSuccessful(responseCode)) {
        throw UserException
          .dataReadError()
          .message("HTTP request failed")
          .addContext("Response code", response.code())
          .addContext("Response message", response.message())
          .addContext(errorContext)
          .build(logger);
      }
      logger.debug("HTTP Request for {} successful.", url());
      logger.debug("Response Headers: {} ", response.headers());

      // Return the InputStream of the response
      return Objects.requireNonNull(response.body()).byteStream();
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to read the HTTP response body")
        .addContext("Error message", e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }
  }

  /**
   * This function is a replacement for the isSuccessful() function which comes
   * with okhttp3.  The issue is that in some cases, a user may not want Drill to throw
   * errors on 400 response codes.  This function will return true/false depending on the
   * configuration for the specific connection.
   *
   * @param responseCode An int of the connection code
   * @return True if the response code is 200-299 and possibly 400-499, false if other
   */
  private boolean isSuccessful(int responseCode) {
    if (scanDefn.tableSpec().connectionConfig().errorOn400()) {
      return ((responseCode >= 200 && responseCode <= 299) ||
          (responseCode >= 400 && responseCode <= 499));
    } else {
      return responseCode >= 200 && responseCode <= 299;
    }
  }

  /**
   * Gets the HTTP response code from the HTTP call.  Note that this value
   * is only available after the getInputStream() method has been called.
   *
   * @return int value of the HTTP response code
   */
  public int getResponseCode() {
    return responseCode;
  }

  /**
   * Gets the HTTP response code from the HTTP call.  Note that this value
   * is only available after the getInputStream() method has been called.
   *
   * @return int of HTTP response code
   */
  public String getResponseMessage() {
    return responseMessage;
  }

  /**
   * Gets the HTTP response code from the HTTP call.  Note that this value
   * is only available after the getInputStream() method has been called.
   *
   * @return The HTTP response protocol
   */
  public String getResponseProtocol() {
    return responseProtocol;
  }

  /**
   * Gets the HTTP response code from the HTTP call.  Note that this value
   * is only available after the getInputStream() method has been called.
   *
   * @return The HTTP response URL
   */
  public String getResponseURL() {
    return responseURL;
  }

  /**
   * Configures response caching using a provided temp directory.
   *
   * @param builder Builder the Builder object to which the caching is to be
   *                configured
   */
  private void setupCache(Builder builder) {
    int cacheSize = 10 * 1024 * 1024;   // TODO Add cache size in MB to config
    File cacheDirectory = new File(tempDir, "http-cache");
    if (!cacheDirectory.exists()) {
      if (!cacheDirectory.mkdirs()) {
        throw UserException
          .dataWriteError()
          .message("Could not create the HTTP cache directory")
          .addContext("Path", cacheDirectory.getAbsolutePath())
          .addContext("Please check the temp directory or disable HTTP caching.")
          .addContext(errorContext)
          .build(logger);
      }
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
  private FormBody.Builder buildPostBody(String postBody) {
    final Pattern postBodyPattern = Pattern.compile("^.+=.+$");

    FormBody.Builder formBodyBuilder = new FormBody.Builder();
    String[] lines = postBody.split("\\r?\\n");
    for (String line : lines) {

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
   * Returns the URL-decoded URL
   *
   * @return Returns the URL-decoded URL
   */
  public static String decodedURL(HttpUrl url) {
    try {
      return URLDecoder.decode(url.toString(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return url.toString();
    }
  }

  /**
   * Returns true if the url has url parameters, as indicated by the presence of
   * {param} in a url.
   *
   * @return True if there are URL params, false if not
   */
  public static boolean hasURLParameters(HttpUrl url) {
    String decodedUrl = SimpleHttp.decodedURL(url);
    Matcher matcher = URL_PARAM_REGEX.matcher(decodedUrl);
    return matcher.find();
  }

  /**
   * APIs sometimes are structured with parameters in the URL itself.  For instance, to request a list of
   * an organization's repositories in github, the URL is: https://api.github.com/orgs/{org}/repos, where
   * you can replace the org with the actual organization name.
   *
   * @return A list of URL parameters enclosed by curly braces.
   */
  public static List<String> getURLParameters(HttpUrl url) {
    String decodedURL = decodedURL(url);
    Matcher matcher = URL_PARAM_REGEX.matcher(decodedURL);
    List<String> parameters = new ArrayList<>();
    String param;
    while (matcher.find()) {
      param = matcher.group();
      param = param.replace("{", "");
      param = param.replace("}", "");
      parameters.add(param);
    }
    return parameters;
  }

  /**
   * Used for APIs which have parameters in the URL.  This function maps the filters pushed down
   * from the query into the URL.  For example the API: github.com/orgs/{org}/repos requires a user to
   * specify an organization and replace {org} with an actual organization.  The filter is passed down from
   * the query.
   *
   * Note that if a URL contains URL parameters and one is not provided in the filters, Drill will throw
   * a UserException.
   *
   * @param url The HttpUrl containing URL Parameters
   * @param filters:  A CaseInsensitiveMap of filters
   * @return A string of the URL with the URL parameters replaced by filter values
   */
  public static String mapURLParameters(HttpUrl url, Map<String, String> filters) {
    if (!hasURLParameters(url)) {
      return url.toString();
    }

    CaseInsensitiveMap<String>caseInsensitiveFilterMap = (CaseInsensitiveMap<String>)filters;

    List<String> params = SimpleHttp.getURLParameters(url);
    String tempUrl = SimpleHttp.decodedURL(url);
    for (String param : params) {
      if (caseInsensitiveFilterMap == null) {
        throw UserException
          .parseError()
          .message("API Query with URL Parameters must be populated. Parameter " + param + " must be included in WHERE clause.")
          .build(logger);
      }

      String value = caseInsensitiveFilterMap.get(param);
      // If the param is not populated, throw an exception
      if (Strings.isNullOrEmpty(value)) {
        throw UserException
          .parseError()
          .message("API Query with URL Parameters must be populated. Parameter " + param + " must be included in WHERE clause.")
          .build(logger);
      } else {
        tempUrl = tempUrl.replace("/{" + param + "}", "/" + value);
      }
    }
    return tempUrl;
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
