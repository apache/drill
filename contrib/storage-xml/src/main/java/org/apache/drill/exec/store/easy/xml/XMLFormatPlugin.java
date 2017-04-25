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
package org.apache.drill.exec.store.easy.xml;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.CoreOperatorType;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.store.easy.xml.XMLFormatPlugin.XMLFormatConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.drill.exec.store.easy.xml.XMLRecordReader;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Created by mpierre on 15-11-04.
 */

public class XMLFormatPlugin extends EasyFormatPlugin<XMLFormatConfig> {

    private static final boolean IS_COMPRESSIBLE = false;
    private static final String DEFAULT_NAME = "xml";
    private Boolean keepPrefix = true;
    private XMLFormatConfig xmlConfig;

    public XMLFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig storageConfig) {
        this(name, context, fsConf, storageConfig, new XMLFormatConfig());
    }

    public XMLFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig config, XMLFormatConfig formatPluginConfig) {
        super(name, context, fsConf, config, formatPluginConfig, true, false, false, IS_COMPRESSIBLE, formatPluginConfig.getExtensions(), DEFAULT_NAME);
        xmlConfig = formatPluginConfig;
    }

    @Override
    public RecordReader getRecordReader(FragmentContext context, DrillFileSystem dfs, FileWork fileWork,
                                        List<SchemaPath> columns, String userName) throws ExecutionSetupException {
        return new XMLRecordReader(context, fileWork.getPath(), dfs, columns, xmlConfig);
    }


    @Override
    public int getReaderOperatorType() {
        return CoreOperatorType.JSON_SUB_SCAN_VALUE;
    }

    @Override
    public int getWriterOperatorType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsPushDown() {
        return true;
    }

    @Override
    public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
        return null;
    }

    @JsonTypeName("xml")
    public static class XMLFormatConfig implements FormatPluginConfig {

        public List<String> extensions;
        public boolean keepPrefix = true;

        private static final List<String> DEFAULT_EXTS = ImmutableList.of("xml");

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        public List<String> getExtensions() {
            if (extensions == null) {
                // when loading an old JSONFormatConfig that doesn't contain an "extensions" attribute
                return DEFAULT_EXTS;
            }
            return extensions;
        }

        @JsonInclude(JsonInclude.Include.NON_DEFAULT)
        public boolean getKeepPrefix() {

            return keepPrefix;
        }

        @Override
        public int hashCode() {
            return 99;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            } else if (obj == null) {
                return false;
            } else if (getClass() == obj.getClass()) {
                return true;
            }

            return false;
        }
    }
}
