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

package org.apache.drill.exec.util;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.drill.common.exceptions.UserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HttpUtils {
  private static final Pattern URL_PARAM_REGEX = Pattern.compile("\\{(\\w+)(?:=(\\w*))?}");
  private static final Logger logger = LoggerFactory.getLogger(HttpUtils.class);
  private static final int DEFAULT_TIMEOUT = 1;

  /**
   * Returns the URL-decoded URL. If the URL is invalid, return the original URL.
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
    String decodedUrl = decodedURL(url);
    Matcher matcher = URL_PARAM_REGEX.matcher(decodedUrl);
    return matcher.find();
  }

  /**
   * APIs are sometimes structured with parameters in the URL itself.  For instance, to request a list of
   * an organization's repositories in github, the URL is: https://api.github.com/orgs/{org}/repos, where
   * you can replace the org with the actual organization name.
   *
   * @return A list of URL parameters enclosed by curly braces.
   */
  public static List<String> getURLParameters(HttpUrl url) {
    String decodedURL = decodedURL(url);
    Matcher matcher = URL_PARAM_REGEX.matcher(decodedURL);
    List<String> parameters = new ArrayList<>();
    while (matcher.find()) {
      String param = matcher.group(1);
      parameters.add(param);
    }
    return parameters;
  }

  /**
   * This function is used to extract the default parameter supplied in a URL. For instance,
   * if the supplied URL is http://someapi.com/path/{p1=foo}, the function will return foo. If there
   * is not a matching parameter or no default value, the function will return null.
   * @param url The URL containing a default parameter
   * @param parameter The parameter for which you need the value
   * @return The value for the supplied parameter
   */
  public static String getDefaultParameterValue (HttpUrl url, String parameter) {
    String decodedURL = decodedURL(url);
    Pattern paramRegex = Pattern.compile("\\{" + parameter + "=(\\w+?)}");
    Matcher paramMatcher = paramRegex.matcher(decodedURL);
    if (paramMatcher.find()) {
      return paramMatcher.group(1);
    } else {
      throw UserException
        .validationError()
        .message("Default URL parameters must have a value. The parameter " + parameter + " is not defined in the configuration.")
        .build(logger);
    }
  }

  public static String mapPositionalParameters(String rawUrl, List<String> params) {
    HttpUrl url = HttpUrl.parse(rawUrl);

    if (!hasURLParameters(url)) {
      return url.toString();
    }

    if (params == null) {
      throw UserException
        .parseError()
        .message("API Query with URL Parameters must be populated.")
        .build(logger);
    }
    String tempUrl = decodedURL(url);
    int startIndex;
    int endIndex;
    int counter = 0;
    while (counter < params.size()) {
      startIndex = tempUrl.indexOf("{");
      endIndex = tempUrl.indexOf("}");

      if (startIndex == -1 || endIndex == -1) {
        break;
      }

      StringBuffer tempUrlBuffer = new StringBuffer(tempUrl);
      tempUrlBuffer.replace(startIndex, endIndex + 1, params.get(counter));
      tempUrl = tempUrlBuffer.toString();
      counter++;
    }
    return tempUrl;
  }

  public static OkHttpClient getSimpleHttpClient() {
    return new OkHttpClient.Builder()
      .connectTimeout(DEFAULT_TIMEOUT, TimeUnit.SECONDS)
      .writeTimeout(DEFAULT_TIMEOUT, TimeUnit.SECONDS)
      .readTimeout(DEFAULT_TIMEOUT, TimeUnit.SECONDS)
      .build();
  }

  public static String makeSimpleGetRequest(String url) {
    OkHttpClient client = getSimpleHttpClient();
    Request.Builder requestBuilder = new Request.Builder()
      .url(url);

    // Build the request object
    Request request = requestBuilder.build();

    // Execute the request
    try {
      Response response = client.newCall(request).execute();
      return response.body().string();
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message("HTTP request failed")
        .build(logger);
    }
  }
}
