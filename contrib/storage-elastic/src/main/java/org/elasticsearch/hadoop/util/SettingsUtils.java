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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException;
import org.elasticsearch.hadoop.cfg.InternalConfigurationOptions;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.serialization.field.FieldFilter.NumberedInclude;

public abstract class SettingsUtils {

    private static List<String> qualifyNodes(String nodes, int defaultPort) {
        List<String> list = StringUtils.tokenize(nodes);
        for (int i = 0; i < list.size(); i++) {
            String nodeIp = resolveHostToIpIfNecessary(list.get(i));
            list.set(i, qualifyNode(nodeIp, defaultPort));
        }
        return list;
    }

    private static String qualifyNode(String node, int defaultPort) {
        int index = node.lastIndexOf(':');
        if (index > 0) {
            if (index + 1 < node.length()) {
                // the port is already in the node
                if (Character.isDigit(node.charAt(index + 1))) {
                    return node;
                }
            }
        }

        return node + ":" + defaultPort;
    }

    private static String resolveHostToIpIfNecessary(String host) {
        String schemaDelimiter = "://";
        String schema = StringUtils.EMPTY;

        if (host.contains(schemaDelimiter)) {
            int index = host.indexOf(schemaDelimiter);
            schema = host.substring(0, index);
            host = host.substring(index + schemaDelimiter.length());
        }
        int index = host.lastIndexOf(':');
        String name = index > 0 ? host.substring(0, index) : host;
        // if the port is specified, include the ":"
        String port = index > 0 ? host.substring(index) : "";
        if (StringUtils.hasLetter(name)) {
            try {
                String hostAddress = InetAddress.getByName(name).getHostAddress() + port;
                return StringUtils.hasText(schema) ? schema + schemaDelimiter + hostAddress : hostAddress;
            } catch (UnknownHostException ex) {
                throw new EsHadoopIllegalArgumentException("Cannot resolve ip for hostname: " + name);
            }
        }
        return host;
    }

    public static void pinNode(Settings settings, String node) {
        pinNode(settings, node, settings.getPort());
    }

    public static void pinNode(Settings settings, String node, int port) {
        if (StringUtils.hasText(node) && port > 0) {
            settings.setProperty(InternalConfigurationOptions.INTERNAL_ES_PINNED_NODE, qualifyNode(node, port));
        }
    }

    public static boolean hasPinnedNode(Settings settings) {
        return StringUtils.hasText(settings.getProperty(InternalConfigurationOptions.INTERNAL_ES_PINNED_NODE));
    }

    public static String getPinnedNode(Settings settings) {
        String node = settings.getProperty(InternalConfigurationOptions.INTERNAL_ES_PINNED_NODE);
        Assert.hasText(node, "Task has not been pinned to a node...");
        return node;
    }

    public static void addDiscoveredNodes(Settings settings, List<String> discoveredNodes) {
        // clean-up and merge
        Set<String> nodes = new LinkedHashSet<String>();
        nodes.addAll(declaredNodes(settings));
        nodes.addAll(discoveredNodes);

        setDiscoveredNodes(settings, nodes);
    }

    public static void setDiscoveredNodes(Settings settings, Collection<String> nodes) {
        settings.setProperty(InternalConfigurationOptions.INTERNAL_ES_DISCOVERED_NODES, StringUtils.concatenate(nodes));
    }

    public static List<String> declaredNodes(Settings settings) {
        return qualifyNodes(settings.getNodes(), settings.getPort());
    }

    public static List<String> discoveredOrDeclaredNodes(Settings settings) {
        // returned the discovered nodes or, if not defined, the set nodes
        String discoveredNodes = settings.getProperty(InternalConfigurationOptions.INTERNAL_ES_DISCOVERED_NODES);
        return (StringUtils.hasText(discoveredNodes) ? StringUtils.tokenize(discoveredNodes) : declaredNodes(settings));
    }

    public static Map<String, String> aliases(String definition, boolean caseInsensitive) {
        List<String> aliases = StringUtils.tokenize(definition);

        Map<String, String> aliasMap = new LinkedHashMap<String, String>();

        if (aliases != null) {
            for (String string : aliases) {
                // split alias
                string = string.trim();
                int index = string.indexOf(":");
                if (index > 0) {
                    String key = string.substring(0, index);
                    aliasMap.put(key, string.substring(index + 1));
                    aliasMap.put(caseInsensitive ? key.toLowerCase(Locale.ROOT) : key, string.substring(index + 1));
                }
            }
        }

        return aliasMap;
    }

    public static void setFilters(Settings settings, String... filters) {
        // clear any filters inside the settings
        settings.setProperty(InternalConfigurationOptions.INTERNAL_ES_QUERY_FILTERS, "");

        if (ObjectUtils.isEmpty(filters)) {
            return;
        }

        settings.setProperty(InternalConfigurationOptions.INTERNAL_ES_QUERY_FILTERS, IOUtils.serializeToBase64(filters));
    }

    public static String[] getFilters(Settings settings) {
        return IOUtils.deserializeFromBase64(settings.getProperty(InternalConfigurationOptions.INTERNAL_ES_QUERY_FILTERS));
    }

    /**
     * Whether the settings indicate a ES 2.x (which introduces breaking changes) or 1.x.
     *
     * @param settings
     * @return
     */
    public static boolean isEs20(Settings settings) {
        String version = settings.getProperty(InternalConfigurationOptions.INTERNAL_ES_VERSION);
        // assume ES 1.0 by default
        if (!StringUtils.hasText(version)) {
            return true;
        }

        return version.startsWith("2.");
    }

    public static List<NumberedInclude> getFieldArrayFilterInclude(Settings settings) {
        String includeString = settings.getReadFieldAsArrayInclude();
        List<String> includes = StringUtils.tokenize(includeString);

        List<NumberedInclude> list = new ArrayList<NumberedInclude>(includes.size());

        for (String include : includes) {
            int index = include.indexOf(":");
            String filter = include;
            int depth = 1;

            try {
            if (index > 0) {
                filter = include.substring(0, index);
                String depthString = include.substring(index + 1);
                if (depthString.length() > 0) {
                    depth = Integer.parseInt(depthString);
                }
            }
            } catch (NumberFormatException ex) {
                throw new EsHadoopIllegalArgumentException(
                        String.format(Locale.ROOT, "Invalid parameter [%s] specified in setting [%s]",
                        include, includeString), ex);
            }
            list.add(new NumberedInclude(filter, depth));
        }

        return list;
    }
}