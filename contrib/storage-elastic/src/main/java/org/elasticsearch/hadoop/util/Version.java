/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.hadoop.util;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.LogFactory;


public abstract class Version {

    private static final String UNKNOWN = "Unknown";
    private static final String VER;
    private static final String HASH;
    private static final String SHORT_HASH;

    public static boolean printed = false;

    static {
        // check classpath
        String target = Version.class.getName().replace(".", "/").concat(".class");
        Enumeration<URL> res = null;

        try {
            res = Version.class.getClassLoader().getResources(target);
        } catch (IOException ex) {
            LogFactory.getLog(Version.class).warn("Cannot detect ES-Hadoop jar; it typically indicates a deployment issue...");
        }

        if (res != null) {
            List<URL> urls = Collections.list(res);
            Set<String> normalized = new LinkedHashSet<String>();

            for (URL url : urls) {
                normalized.add(StringUtils.normalize(url.toString()));
            }

            int foundJars = 0;
            if (normalized.size() > 1) {
                StringBuilder sb = new StringBuilder("Multiple ES-Hadoop versions detected in the classpath; please use only one\n");
                for (String s : normalized) {
                    if (s.contains("jar:")) {
                        foundJars++;
                        sb.append(s.replace("!/" + target, ""));
                        sb.append("\n");
                    }
                }
                if (foundJars > 1) {
                    LogFactory.getLog(Version.class).fatal(sb);
                    throw new Error(sb.toString());
                }
            }
        }

        Properties build = new Properties();
        try {
            build = IOUtils.propsFromString(IOUtils.asString(Version.class.getResourceAsStream("/esh-build.properties")));
        } catch (Exception ex) {
            // ignore if no build info was found
        }
        VER = build.getProperty("version", UNKNOWN);
        HASH = build.getProperty("hash", UNKNOWN);
        SHORT_HASH = HASH.length() > 10 ? HASH.substring(0, 10) : HASH;
    }

    public static String version() {
        return "v" + versionNumber() + " [" + versionHashShort() + "]";
    }

    public static String versionNumber() {
        return VER;
    }

    public static String versionHash() {
        return HASH;
    }

    public static String versionHashShort() {
        return SHORT_HASH;
    }

    public static void logVersion() {
        if (!printed) {
            printed = true;
            LogFactory.getLog(Version.class).info("Elasticsearch Hadoop " + version());
        }
    }
}
