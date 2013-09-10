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

import com.google.common.collect.Lists;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.jet.random.Exponential;

import java.net.InetAddress;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: tdunning
 * Date: 2/2/13
 * Time: 6:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class User {
    private Exponential queryLengthDistribution = new Exponential(0.4, RandomUtils.getRandom());

    private long cookie = RandomUtils.getRandom().nextLong();

    private TermGenerator terms;
    private InetAddress address;
    private String geoCode;

    public User(InetAddress address, TermGenerator geoCoder, TermGenerator terms) {
        this.terms = terms;
        geoCode = geoCoder.sample();
        this.address = address;
    }

    public InetAddress getAddress() {
        return address;
    }

    public long getCookie() {
        return cookie;
    }

    public List<String> getQuery() {
        int n = queryLengthDistribution.nextInt() + 1;
        List<String> r = Lists.newArrayList();
        for (int i = 0; i < n; i++) {
            r.add(terms.sample());
        }
        return r;
    }

    public String getGeoCode() {
        return geoCode;
    }

    @Override
    public String toString() {
        return String.format("{ip:\"%s\", cookie:\"%08x\", geo:\"%s\"}", address, cookie, geoCode);
    }
}
