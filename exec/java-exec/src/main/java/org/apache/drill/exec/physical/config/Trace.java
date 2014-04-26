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

package org.apache.drill.exec.physical.config;

import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractSingle;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.Size;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName("trace")
public class Trace extends AbstractSingle {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Trace.class);

    /* Tag associated with each trace operator
     * Printed along with trace output to distinguish
     * between multiple trace operators within same plan
     */
    public final String traceTag;

    public Trace(@JsonProperty("child") PhysicalOperator child, @JsonProperty("tag") String traceTag) {
        super(child);
        this.traceTag = traceTag;
    }

    @Override
    public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
        return physicalVisitor.visitTrace(this, value);
    }

    @Override
    public OperatorCost getCost() {

        /* Compute the total size (row count * row size) */
        Size size     = child.getSize();
        long diskSize = size.getRecordCount() * size.getRecordSize();

        return new OperatorCost(0, diskSize, 0, child.getSize().getRecordCount());
    }

    @Override
    public Size getSize() {
        return child.getSize();
    }

    @Override
    protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
        return new Trace(child, traceTag);
    }
}
