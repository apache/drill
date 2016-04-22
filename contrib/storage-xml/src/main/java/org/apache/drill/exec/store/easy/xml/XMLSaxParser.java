/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.store.easy.xml;


import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import java.util.Stack;

public class XMLSaxParser extends DefaultHandler {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(XMLSaxParser.class);
    private JSONObject val = new JSONObject();
    private JSONObject newVal;
    private String content;
    private Stack stk = new Stack();
    private Stack key_stack = new Stack();
    private Stack removal_stk = new Stack();
    private boolean removeNameSpace = false;
    public boolean isRemoveNameSpace() {
        return removeNameSpace;
    }
    public void setRemoveNameSpace(boolean removeNameSpace) {
        this.removeNameSpace = removeNameSpace;
    }

    public JSONObject getVal() {

        String stringkey;
        // Due to limitations in Drill, I simplify the model by removing all nodes that are empty
        while(!key_stack.isEmpty() && !removal_stk.empty()) {
            stringkey = (String) key_stack.pop();
            JSONObject o = (JSONObject) removal_stk.pop();
            JSONObject val = (JSONObject) o.get(stringkey);
            if(val.size() > 0) {
                logger.info("XmlPlugin Parser: Object:" + val.toJSONString() + "Not null which is not expected.");
            }
            else {
                o.remove(stringkey);
            }
        }
        return val;
    }



    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        super.startElement(uri, localName, qName, attributes);

        JSONObject tag = new JSONObject();
        content = null;

        int length = attributes.getLength();
        for (int i = 0; i < length; i++) {
            String qNameVal = attributes.getQName(i);
            String valueVal = attributes.getValue(i);

            if (qNameVal != null) {
                qNameVal = '@' + cleanQName(qNameVal);
            }
            if (valueVal.trim().length() > 0) {
                tag.put(qNameVal, valueVal);
            }
        }

        stk.push(tag);
    }


    public String cleanQName(String qName) {
        if (isRemoveNameSpace()) {
            int pos = qName.lastIndexOf(":");
            return qName.substring(pos + 1);

        }

        return qName;
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);

        String cleanQName = cleanQName(qName);

        if (stk.empty()) {
            stk.push(val);
        }

        newVal = (JSONObject) stk.pop();
        if (content != null) {
            newVal.put("#value", content);
            content = null;
        }

        if (!stk.empty()) {
            // Get the parent from the stack
            JSONObject parent = (JSONObject) stk.pop();
            if (parent.containsKey(cleanQName) || parent.containsKey(cleanQName.trim()+":drill_array")) {
                int arrayExists = 0;
                if(parent.containsKey(cleanQName.trim()+":drill_array")) {
                    arrayExists = 1;
                }

                switch(arrayExists) {
                    case 1:
                        JSONArray old_array = (JSONArray) parent.get(cleanQName.trim()+":drill_array");
                        addNodeToArray(parent, cleanQName,old_array,newVal);
                        break;

                    case 0:
                        if (parent.get(cleanQName) instanceof JSONValue ||parent.get(cleanQName) instanceof JSONObject ) {
                            JSONObject old_val =  (JSONObject) parent.get(cleanQName);
                            parent.remove(cleanQName);
                            addNodeToArray(parent, cleanQName, old_val, newVal);

                        } else {
                            String old_val = (String) parent.get(cleanQName);
                            addNodeToArray(parent, cleanQName, old_val, newVal);
                        }
                        break;
                }
            }
            else {
                if(newVal.size() == 0) {
                    parent.put(cleanQName, newVal);
                   // Tracking empty nodes so we can clean them out
                   // to help drill with schema consolidation
                    removal_stk.push(parent);
                    key_stack.push(cleanQName);
                }
                else {
                   parent.put(cleanQName, newVal);
               }
                stk.push(parent);
            }
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);
        if (content == null) {
            content = new String(ch, start, length);
        } else {
            content += new String(ch, start, length);
        }
        content = content.trim().replace("\n", "");
        if (content.length() == 0) {
            content = null;
        }

    }

    private int addNodeToArray(JSONObject parent,String cleanQName, Object old, Object new_val) {
        String cleanQNameArray = cleanQName.trim()+":drill_array";
        if(!key_stack.empty()) {
            String old_objkey = (String) key_stack.peek();
            if (cleanQName.equals(old_objkey)) {
                key_stack.pop();
                removal_stk.pop();
            }
        }

        if(old instanceof JSONArray) {
            JSONArray old_array = (JSONArray) old;
            JSONObject newVal = (JSONObject) new_val;
            if(newVal.size() == 1  && newVal.containsKey("#value")) {
                old_array.add(newVal.get("#value"));
            }
            else {
                old_array.add(newVal);
            }
            parent.put(cleanQNameArray, old_array);

        }
        else if(old instanceof JSONObject) {
            JSONObject old_val = (JSONObject) old;
            JSONArray new_array = new JSONArray();
            JSONObject newVal = (JSONObject) new_val;

            if(old_val.size() == 1 && old_val.containsKey("#value")) {
                new_array.add(old_val.get("#value"));
            }
            else {
                new_array.add(old_val);
            }

            if(newVal.size() == 1 && newVal.containsKey("#value")) {
                new_array.add(newVal.get("#value"));
            }
            else {
                new_array.add(newVal);
            }
            parent.put(cleanQNameArray, new_array);
        }
        else if(old instanceof String) {
            JSONArray new_array = new JSONArray();
            String old_val = (String) old;
            new_array.add(old_val);
            parent.remove(cleanQName);
            if(newVal.size() == 1) {
                new_array.add(newVal.get("#value"));
            } else {
                new_array.add(newVal);
            }
            parent.put(cleanQNameArray, new_array);
        }
        stk.push(parent);
        return 0;
    }

    @Override
    public void startDocument() throws SAXException {
        super.startDocument();
        val = new JSONObject();
        stk.push(val);
    }

    @Override
    public void endDocument() throws SAXException {
        super.endDocument();
    }
}
