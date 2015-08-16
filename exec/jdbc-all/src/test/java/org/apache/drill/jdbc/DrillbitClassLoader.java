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
package org.apache.drill.jdbc;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class DrillbitClassLoader extends URLClassLoader {

  public DrillbitClassLoader() {
    super(URLS);
  }

  private static final URL[] URLS;

  static {
    ArrayList<URL> urlList = new ArrayList<URL>();
    final String classPath = System.getProperty("app.class.path");
    final String[] st = fracture(classPath, File.pathSeparator);
    final int l = st.length;
    for (int i = 0; i < l; i++) {
      try {
        if (st[i].length() == 0) {
          st[i] = ".";
        }
        urlList.add(new File(st[i]).toURI().toURL());
      } catch (MalformedURLException e) {
        assert false : e.toString();
      }
    }
    urlList.toArray(new URL[urlList.size()]);

    List<URL> urls = new ArrayList<>();
    for (URL url : urlList) {
      urls.add(url);
    }
    URLS = urls.toArray(new URL[urls.size()]);
  }

  /**
   * Helper method to avoid StringTokenizer using.
   *
   * Taken from Apache Harmony
   */
  private static String[] fracture(String str, String sep) {
    if (str.length() == 0) {
      return new String[0];
    }
    ArrayList<String> res = new ArrayList<String>();
    int in = 0;
    int curPos = 0;
    int i = str.indexOf(sep);
    int len = sep.length();
    while (i != -1) {
      String s = str.substring(curPos, i);
      res.add(s);
      in++;
      curPos = i + len;
      i = str.indexOf(sep, curPos);
    }

    len = str.length();
    if (curPos <= len) {
      String s = str.substring(curPos, len);
      in++;
      res.add(s);
    }

    return res.toArray(new String[in]);
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    return super.findClass(name);
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return super.loadClass(name);
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    return super.loadClass(name, resolve);
  }

}
