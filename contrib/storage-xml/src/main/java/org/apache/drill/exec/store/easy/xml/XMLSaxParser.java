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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.Stack;

/**
 * Created by mpierre on 15-11-04.
 */

public class XMLSaxParser extends DefaultHandler {


    private Logger log;


    private JSONObject val = new JSONObject();
    private JSONObject newVal;
    private String content;
    private Stack stk = new Stack();
    private boolean removeNameSpace = false;

    public boolean isRemoveNameSpace() {
        return removeNameSpace;
    }

    public void setRemoveNameSpace(boolean removeNameSpace) {
        this.removeNameSpace = removeNameSpace;
    }

    public JSONObject getVal() {
        return val;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        super.startElement(uri, localName, qName, attributes);

        JSONObject tag = new JSONObject();
        JSONArray array = new JSONArray();
        content = null;

        int length = attributes.getLength();
        for(int i = 0; i<  length; i++) {
            JSONObject temp = new JSONObject();
            String qNameVal = attributes.getQName(i);
            String valueVal = attributes.getValue(i);

            if(qNameVal != null) {
                qNameVal = cleanQName(qNameVal);
            }
            if(valueVal.trim().length() > 0) {
                tag.put(qNameVal, valueVal);
                //array.add(temp);
            }
        }

        if(array.size() > 0) {
            tag.put("attributes", array);
            // tag.put("qName",cleanQName(qName));
        }
        //log.info("After clean:" + cleanQName(qName));
        stk.push(tag);
    }


    public String cleanQName(String qName) {
        if (isRemoveNameSpace() == true) {
            //log.info("Before: " + qName);

            int pos = qName.lastIndexOf(":");
            //log.info("pos:" + pos);
            return qName.substring(pos+1);

        }

        return qName;
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);

        String cleanQName = cleanQName(qName);

        if(stk.empty() == true) {
            //System.out.println("Stack empty");
            stk.push(val);
        }
        newVal =(JSONObject) stk.pop();
        if(content != null) {

            newVal.put("value", content);
            content=null;
        }

        if(stk.empty() == false) {
            JSONObject parent = (JSONObject) stk.pop();

            if(parent.containsKey(cleanQName) == true ) {
                if(parent.get(cleanQName) instanceof JSONObject ) {
                    JSONObject old_val = (JSONObject) parent.get(cleanQName);
                    if(newVal.size() > 0) {
                        JSONArray new_array = new JSONArray();

                        if(old_val.size() == 1 && old_val.containsKey("value") &&
                                newVal.size()==1 && newVal.containsKey("value")) {
                            new_array.add(old_val.get("value"));
                            new_array.add(newVal.get("value"));
                        } else {
                            new_array.add(old_val);
                            new_array.add(newVal);
                        }
                        parent.put(cleanQName, new_array);
                    }

                    stk.push(parent);
                }
                else
                if(parent.get(cleanQName) instanceof JSONValue) {
                    JSONValue old_val = (JSONValue) parent.get(cleanQName);
                    JSONArray new_array = new JSONArray();
                    new_array.add(old_val);
                    new_array.add(newVal);


                    parent.put(cleanQName, new_array);
                    stk.push(parent);


                } else if(parent.get(cleanQName) instanceof JSONArray) {
                    JSONArray old_val = (JSONArray) parent.get(cleanQName);
                    if(newVal.size() == 1 && newVal.containsKey("value")) {
                        old_val.add(newVal.get("value"));
                    } else {
                        old_val.add(newVal);
                    }
                    stk.push(parent);
                } else {
                    String old_val = (String) parent.get(cleanQName);
                    JSONArray new_array = new JSONArray();
                    new_array.add(old_val);
                    new_array.add(newVal);

                    parent.put(cleanQName, new_array);
                    stk.push(parent);
                }
            } else {
                parent.put(cleanQName, newVal);
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
    }



    @Override
    public void startDocument() throws SAXException {
        super.startDocument();
        val = new JSONObject();
        stk.push(val);
    }

}
