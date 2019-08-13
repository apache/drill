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

package org.apache.drill.exec.udfs;

import com.maxmind.geoip2.DatabaseReader;
import org.apache.drill.common.exceptions.UserException;
import java.util.HashMap;


public class SecurityHelperFunctions {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SecurityHelperFunctions.class);

  public static DatabaseReader getCountryDatabaseReader() throws UserException {
    java.io.InputStream db = SecurityHelperFunctions.class.getClassLoader().getResourceAsStream("GeoLite2-Country.mmdb");
    try {
      DatabaseReader reader = new com.maxmind.geoip2.DatabaseReader.Builder(db).withCache(new com.maxmind.db.CHMCache()).build();
      return reader;
    } catch (java.io.IOException e) {
      throw UserException.validationError().message("Could not locate MaxMind Country Database.  Please ensure that it is in your classpath.").build(logger);
    }
  }

  public static DatabaseReader getCityDatabaseReader() throws UserException {
    java.io.InputStream db = SecurityHelperFunctions.class.getClassLoader().getResourceAsStream("GeoLite2-City.mmdb");
    try {
      DatabaseReader reader = new com.maxmind.geoip2.DatabaseReader.Builder(db).withCache(new com.maxmind.db.CHMCache()).build();
      return reader;
    } catch (java.io.IOException e) {
      throw UserException.validationError().message("Could not locate MaxMind City Database.  Please ensure that it is in your classpath.").build(logger);
    }
  }

  public static DatabaseReader getASNDatabaseReader() throws UserException {
    java.io.InputStream db = SecurityHelperFunctions.class.getClassLoader().getResourceAsStream("GeoLite2-ASN.mmdb");
    try {
      DatabaseReader reader = new com.maxmind.geoip2.DatabaseReader.Builder(db).withCache(new com.maxmind.db.CHMCache()).build();
      return reader;
    } catch (java.io.IOException e) {
      throw UserException.validationError().message("Could not locate MaxMind City Database.  Please ensure that it is in your classpath.").build(logger);
    }
  }

  public static HashMap getPortHashMap() throws UserException {
    java.io.InputStream serviceFile = SecurityHelperFunctions.class.getClassLoader().getResourceAsStream("service-names-port-numbers.csv");
    HashMap serviceInfo = new java.util.HashMap<String, String>();

    String line = "";
    String key = "";
    String linePattern = "^\\w*,\\d+,";
    try {
      java.io.BufferedReader br = new java.io.BufferedReader(new java.io.InputStreamReader(serviceFile));
      while ((line = br.readLine()) != null) {

        // Create a Pattern object
        java.util.regex.Pattern r = java.util.regex.Pattern.compile(linePattern);

        // Now create matcher object.
        java.util.regex.Matcher m = r.matcher(line);
        int pos;
        String description;
        if (m.find()) {
          String[] values = line.split(",");
          pos = Integer.parseInt(values[1]);
          if (values.length == 3) {
            description = "";
          } else {
            description = values[3];
          }
          key = values[1] + ":" + values[2];
          serviceInfo.put(key, description);
        }
      }
      return serviceInfo;

    } catch (Exception e) {
      throw UserException.validationError().message("Could not read ISO port lookup table.").build(logger);
    }
  }
}
