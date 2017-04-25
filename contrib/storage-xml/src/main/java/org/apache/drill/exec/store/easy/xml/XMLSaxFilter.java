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


import java.util.List;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.Stack;
import org.apache.drill.common.expression.SchemaPath;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLFilterImpl;


/***
 * Purpose of this class is pre-filter the XML in streaming fashion
 * to only contain the tags requested by in the SQL statement.
 * The way it does this is that it traverses the known list of tags
 * from the SQL statement and keeps track of where we are in the process
 * of reading the XML by using two stacks.
 * Stack 1: FilterStack are the tags received from the query, each entry is a set since multiple paths
 * exist in one query.
 * Stack 2: CurrentPosStack is where we are currently.
 * Whenever we are on a tag to be included we send the event to the parent which will call
 * the same event in the SAX parser that produces JSON.
 * Caveat: Function calls such as flatten hides part of the path forcing us to return more data
 * than strictly necessary to the SAX parser.
 */

public class XMLSaxFilter extends XMLFilterImpl {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(XMLSaxFilter.class);
    private Stack<Set<String>> filterStack = new Stack<Set<String>>();
    private Stack<Set<String>> currentPosStack = new Stack<Set<String>>();
    private Set<String> lastFilterPos;
    boolean inKeptElement = false;
    private boolean removeNameSpace = false;

    public boolean isRemoveNameSpace() {
        return removeNameSpace;
    }
    public void setRemoveNameSpace(boolean removeNameSpace) {
        this.removeNameSpace = removeNameSpace;
    }

    public String cleanQName(String qName) {
        if (isRemoveNameSpace()) {
            int pos = qName.lastIndexOf(":");
            return qName.substring(pos + 1);

        }

        return qName;
    }

    public XMLSaxFilter(XMLReader parent, List<SchemaPath> columns) {
        super(parent);
        ArrayList<HashSet<String>> col_sets = new ArrayList<HashSet<String>>();

        Iterator iter = columns.iterator();
        while(iter.hasNext()) {
            SchemaPath obj = (SchemaPath) iter.next();
            String pathStr = obj.toString();

            if(pathStr.contains((CharSequence) new StringBuffer("*"))) {
                return;
            }
            logger.debug("XML Filter: Path Before cleanup: " + pathStr);
            pathStr = pathStr.replace(":drill_array","");
            pathStr = pathStr.replaceAll("\\[.*\\]", "");
            logger.debug("XML Filter: Path After cleanup: " + pathStr);
            String[] splitStr = pathStr.toString().split("`\\.`");

            for(int i = 0; i < splitStr.length; i++) {
                if(col_sets.size() <= i) {
                    HashSet<String> cols = new HashSet<String>();
                    cols.add(splitStr[i].replace('`', ' ').trim());
                    if(i > 0) {
                        cols.add(splitStr[i].replace('`',' ').trim());
                    }
                    col_sets.add(cols);
                } else {
                    HashSet<String> cols = col_sets.get(i);
                    if(cols != null) {
                        cols.add(splitStr[i].replace('`',' ').trim());
                    } else {
                        // error
                    }
                }
            }
        }

        int col_num = col_sets.size() - 1;
        while(col_num >=0) {
            filterStack.push(col_sets.get(col_num));
            --col_num;
        }

        lastFilterPos = filterStack.peek();
    }


    public void startElement(String uri, String localName, String qName,
                             Attributes atts)
            throws SAXException {

        String cleanQName = cleanQName(qName);

        if(filterStack.isEmpty() == true)  {
            // This means that all filters are "consumed" and that
            // we are in parts of the tree where everything should be kept
            Set val = new HashSet<String>(1);
            val.add(cleanQName);
            currentPosStack.push(val);
            inKeptElement = true;
            super.startElement(uri, localName, cleanQName, atts);
        }
        else if (filterStack.peek().contains(cleanQName)) {
            // If we find a tag we have defined in the query
            // We move it over to the currentPositionStack
            lastFilterPos = filterStack.pop();
            currentPosStack.push(lastFilterPos);
            inKeptElement = true;
            super.startElement(uri, localName, cleanQName, atts);
        }
        else {
            // Don't do anything... prevents processing of elements
        }
    }

    public void endElement(String uri, String localName, String qName)
            throws SAXException {
        String cleanQName = cleanQName(qName);

        if (currentPosStack.empty() == false && currentPosStack.peek().contains(cleanQName)) {
            // We are to close a tag that we have in our stack

            if(currentPosStack.peek().equals(lastFilterPos)) {
                // If it is a tag that was sent into the class to begin with
                // we want to restore the filter so it takes effect again
                filterStack.push(currentPosStack.pop());
                if(!currentPosStack.isEmpty()) {
                    lastFilterPos = currentPosStack.peek();
                }
                super.endElement(uri, localName, cleanQName);
                inKeptElement = false;

            }
            else {
                currentPosStack.pop();
                super.endElement(uri, localName, cleanQName);
                inKeptElement = false;
            }
        }
    }

    public void characters(char ch[], int start, int len)
            throws SAXException {
        if (inKeptElement) {
            super.characters(ch, start, len);
        }
    }

    public void startDocument() throws SAXException {
        super.startDocument();
    }
    public void EndDocument() throws SAXException {
        super.endDocument();
    }

}