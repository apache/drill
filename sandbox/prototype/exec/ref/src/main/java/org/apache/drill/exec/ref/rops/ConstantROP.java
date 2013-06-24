/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.exec.ref.rops;

import java.util.Iterator;

import org.apache.drill.common.logical.data.Constant;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.RunOutcome.OutcomeType;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.exceptions.SetupException;
import org.apache.drill.exec.ref.rse.JSONRecordReader;

import com.fasterxml.jackson.databind.JsonNode;

public class ConstantROP extends ROPBase<Constant>{
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ScanROP.class);

    private UnbackedRecord record;

    public ConstantROP(Constant config) {
        super(config);
        record = new UnbackedRecord();
    }


    @Override
    protected void setupIterators(IteratorRegistry registry) throws SetupException {
       // try{
            super.setupIterators(registry);
            // need to assign reader
           // throw new IOException();
        //}catch(IOException e){
            //throw new SetupException("Failure while setting up reader.");
        //}
    }


    @Override
    protected RecordIterator getIteratorInternal() {
        return new ConstantIterator(ConstantROP.this.config.getContent().getRoot());
    }


    @Override
    public void cleanup(OutcomeType outcome) {
        super.cleanup(outcome);
    }


    class ConstantIterator implements RecordIterator {

        Iterator<JsonNode> jsonIter;

        ConstantIterator(JsonNode json) {
            jsonIter = json.elements();
        }

        public RecordPointer getRecordPointer(){
            return record;
        }

        public NextOutcome next(){
            if ( ! jsonIter.hasNext()){
                return NextOutcome.NONE_LEFT;
            }
            JsonNode contentJSON = ConstantROP.this.config.getContent().getRoot();
            if (contentJSON.isArray())
            { // list of constant records was specified
                JsonNode node;
                node = jsonIter.next();
                convertJsonToRP(node, record);
                return NextOutcome.INCREMENTED_SCHEMA_UNCHANGED;
            }
            else{
                convertJsonToRP(contentJSON, record);
                return NextOutcome.NONE_LEFT;
            }
        }

        private void convertJsonToRP(JsonNode node, RecordPointer rp){
            record.clear();
            record.merge(JSONRecordReader.convert(node));
        }

        public ROP getParent(){
            return ConstantROP.this;
        }

    }

}