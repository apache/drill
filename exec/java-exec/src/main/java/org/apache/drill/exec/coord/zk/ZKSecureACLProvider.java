/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.coord.zk;

import com.google.common.collect.ImmutableList;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;

import java.util.List;

/**
 * ZKSecureACLProvider restricts access to znodes created by Drill in a secure installation.
 *
 * The cluster discovery znode i.e. the znode containing the list of Drillbits is
 * readable by anyone.
 *
 * For all other znodes, only the creator of the znode, i.e the Drillbit user, has full access.
 *
 */

public class ZKSecureACLProvider implements ACLProvider {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZKSecureACLProvider.class);

    /**
     * DEFAULT_ACL gives the creator of a znode full-access to it
     */
    static ImmutableList<ACL> DEFAULT_ACL = new ImmutableList.Builder<ACL>()
                                              .addAll(Ids.CREATOR_ALL_ACL.iterator())
                                              .build();
    /**
     * DRILL_CLUSTER_ACL gives the creator full access and everyone else only read access.
     * Used on the Drillbit discovery znode (znode path /<drill.exec.zk.root>/<drill.exec.cluster-id>)
     * i.e. the node that contains the list of Drillbits in the cluster.
     */
     static ImmutableList<ACL> DRILL_CLUSTER_ACL = new ImmutableList.Builder<ACL>()
                                                .addAll(Ids.READ_ACL_UNSAFE.iterator())
                                                .addAll(Ids.CREATOR_ALL_ACL.iterator())
                                                .build();
    final String clusterName;
    final String drillZkRoot;
    final String drillClusterPath;

    public ZKSecureACLProvider(String clusterName, String drillZKRoot) {
        this.clusterName = clusterName;
        this.drillZkRoot = drillZKRoot;
        this.drillClusterPath = "/" + this.drillZkRoot + "/" +  this.clusterName ;
    }

    public List<ACL> getDefaultAcl() {
        return DEFAULT_ACL;
    }

    public List<ACL> getAclForPath(String path) {
        logger.trace("ZKSecureACLProvider: getAclForPath " + path);
        if(path.equals(drillClusterPath)) {
            logger.trace("ZKSecureACLProvider: getAclForPath drillClusterPath " + drillClusterPath);
            return DRILL_CLUSTER_ACL;
        }
        return DEFAULT_ACL;
    }

}
