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
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.easy.json.JSONRecordReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.util.List;

public class XMLRecordReader extends AbstractRecordReader {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(XMLRecordReader.class);
    private XMLSaxParser handler;
    private SAXParser xmlParser;
    private JsonNode node;
    JSONRecordReader reader = null;
    public XMLRecordReader(FragmentContext fragmentContext, String inputPath, DrillFileSystem fileSystem, List<SchemaPath> columns, XMLFormatPlugin.XMLFormatConfig xmlConfig) throws OutOfMemoryException {
        try {
            FSDataInputStream fsStream = fileSystem.open(new Path(inputPath));
            SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
            xmlParser = saxParserFactory.newSAXParser();
            handler = new XMLSaxParser();

            XMLSaxFilter filter = new XMLSaxFilter(xmlParser.getXMLReader(),columns);
            filter.setContentHandler(handler);
            filter.setRemoveNameSpace(!xmlConfig.getKeepPrefix());
            filter.parse(new InputSource(fsStream.getWrappedStream()));

            ObjectMapper mapper = new ObjectMapper();
            node = mapper.valueToTree(handler.getVal());
            reader = new JSONRecordReader(fragmentContext,(String) null,node,fileSystem,columns);

            logger.debug("XML Plugin, Produced JSON:" + handler.getVal().toJSONString());
            xmlParser = null;
            handler = null;
        }
        catch (SAXException | ParserConfigurationException | IOException e) {
            logger.debug("XML Plugin:" + e.getMessage());

        }
    }


    public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
        reader.setup(context, output);

    }

    protected void handleAndRaise(String suffix, Exception e) throws UserException {
        reader.handleAndRaise(suffix, e);
    }


    public int next() {
        return reader.next();
    }


    public void close() throws Exception {
        reader.close();
    }
}
