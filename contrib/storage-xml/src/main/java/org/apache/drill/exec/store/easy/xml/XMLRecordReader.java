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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.easy.json.JSONRecordReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.util.List;


public class XMLRecordReader extends JSONRecordReader {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(XMLRecordReader.class);
    private XMLSaxParser handler;
    private SAXParser xmlParser;
    private JsonNode node;

    public XMLRecordReader(FragmentContext fragmentContext, String inputPath, DrillFileSystem fileSystem, List<SchemaPath> columns, XMLFormatPlugin.XMLFormatConfig xmlConfig) throws OutOfMemoryException {
        super(fragmentContext, inputPath, fileSystem, columns);
        try {
            FSDataInputStream fsStream = fileSystem.open(new Path(inputPath));
            SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
            xmlParser = saxParserFactory.newSAXParser();
            handler = new XMLSaxParser();
            handler.setRemoveNameSpace(!xmlConfig.getKeepPrefix());
            xmlParser.parse(fsStream.getWrappedStream(), handler);
            ObjectMapper mapper = new ObjectMapper();
            node = mapper.valueToTree(handler.getVal());
            logger.debug("XML Plugin, Produced JSON:" + handler.getVal().toJSONString());
            xmlParser = null;
            handler = null;
            saxParserFactory = null;
            super.stream = null;
            super.embeddedContent = node;
            super.hadoopPath = null;
        }
        catch (SAXException | ParserConfigurationException | IOException e) {
            logger.debug("XML Plugin:" + e.getMessage());

        }
    }


    @Override
    public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
        super.setup(context, output);

    }

    @Override
    protected void handleAndRaise(String suffix, Exception e) throws UserException {
        super.handleAndRaise(suffix, e);
    }

    @Override
    public int next() {
        return super.next();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
