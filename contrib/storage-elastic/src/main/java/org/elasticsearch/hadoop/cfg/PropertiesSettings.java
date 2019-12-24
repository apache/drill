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
package org.elasticsearch.hadoop.cfg;

import java.io.InputStream;
import java.util.Properties;

import org.elasticsearch.hadoop.util.Assert;
import org.elasticsearch.hadoop.util.IOUtils;

/**
 * Properties-based settings - used for testing or local environments (including Cascading-local).
 */
public class PropertiesSettings extends Settings {

    protected final Properties props;

    public PropertiesSettings() {
        this(new Properties());
    }

    public PropertiesSettings(Properties props) {
        Assert.notNull(props, "Non-null properties expected");
        this.props = props;
    }

    @Override
    public String getProperty(String name) {
        return props.getProperty(name);
    }

    @Override
    public void setProperty(String name, String value) {
        props.setProperty(name, value);
    }

    @Override
    public Settings copy() {
        return new PropertiesSettings((Properties) props.clone());
    }

    @Override
    public InputStream loadResource(String location) {
        return IOUtils.open(location, PropertiesSettings.class.getClassLoader());
    }

    @Override
    public Properties asProperties() {
        return props;
    }
}
