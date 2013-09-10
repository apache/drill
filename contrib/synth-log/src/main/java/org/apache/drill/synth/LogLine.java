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
package org.apache.drill.synth;

import java.net.InetAddress;
import java.util.Formatter;
import java.util.List;

/**
 * A log line contains a user id, an IP address and a query.
 */
public class LogLine {
    private InetAddress ip;
    private long cookie;
    private List<String> query;

    public LogLine(InetAddress ip, long cookie, List<String> query) {
        this.cookie = cookie;
        this.ip = ip;
        this.query = query;
    }

    public LogLine(User user) {
        this(user.getAddress(), user.getCookie(), user.getQuery());
    }

    @Override
    public String toString() {
        Formatter r = new Formatter();
        r.format("{cookie:\"%08x\", ip:\"%s\", query:", cookie, ip.getHostAddress());
        String sep = "[";
        for (String term : query) {
            r.format("%s\"%s\"", sep, term);
            sep = ", ";
        }
        r.format("]}");
        return r.toString();
    }
}
