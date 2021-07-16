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
package org.apache.drill.exec.store;

import org.apache.commons.io.FileUtils;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.BaseTest;
import org.junit.After;
import org.junit.ClassRule;

import java.util.HashMap;
import java.util.Map;

public class BasePluginRegistry extends BaseTest {
    @ClassRule
    public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

    protected static final String SYS_PLUGIN_NAME = "sys";
    protected static final String S3_PLUGIN_NAME = "s3";

    // Mixed-case name used to verify that names are forced to lower case.
    protected static final String MY_PLUGIN_NAME = "myPlugin";

    // Lower-case form after insertion into the registry.
    protected static final String MY_PLUGIN_KEY = MY_PLUGIN_NAME.toLowerCase();

    @After
    public void cleanup() throws Exception {
        FileUtils.cleanDirectory(dirTestWatcher.getStoreDir());
    }

    protected FileSystemConfig myConfig1() {
        FileSystemConfig config = new FileSystemConfig("myConn",
                new HashMap<>(), new HashMap<>(), new HashMap<>(),
                PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER);
        config.setEnabled(true);
        return config;
    }

    protected FileSystemConfig myConfig2() {
        Map<String, String> props = new HashMap<>();
        props.put("foo", "bar");
        FileSystemConfig config = new FileSystemConfig("myConn",
                props, new HashMap<>(), new HashMap<>(),
                PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER);
        config.setEnabled(true);
        return config;
    }

}
