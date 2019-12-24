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

package org.apache.drill.exec.store.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import org.elasticsearch.client.Response;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Static Helper class to deal with better search capabilities over Json navigation
 */
public class JsonHelper {

    /**
     * Find the node within the path given (separate fieldnames with '.'). It doesn't support cardinality.
     * @param node
     * @param path path to child fields separated by '.' each.
     * @return a node that points to the path required, or a {@link com.fasterxml.jackson.databind.node.MissingNode}
     */
    public static JsonNode getPath(@NotNull JsonNode node, @NotNull  String path) {
        Preconditions.checkArgument(node != null);
        Preconditions.checkArgument(path != null);
        Iterator<String> fieldIterator = Arrays.asList(path.split("\\.")).iterator();
        JsonNode innerNode = node;
        // 一层一层下去
        while (!innerNode.isMissingNode() && fieldIterator.hasNext()) {
            node = node.path(fieldIterator.next());
        }

        return node;
    }

    public static JsonNode readRespondeContentAsJsonTree(ObjectMapper mapper, Response response) throws IOException {
        Preconditions.checkArgument(mapper != null);
        Preconditions.checkArgument(response != null);
        Preconditions.checkArgument(response.getEntity() != null);
        Preconditions.checkArgument(response.getEntity().getContent() != null);
        // 读取数据
        return mapper.readTree(response.getEntity().getContent());
    }
    
    public static JsonNode readRespondeContentAsJsonTree(ObjectMapper mapper, InputStream in) throws IOException {
        Preconditions.checkArgument(mapper != null);
        // 读取数据
        return mapper.readTree(in);
    }
}
