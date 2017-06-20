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

import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;


public class ZKACLProviderFactory {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ZKACLProviderFactory.class);

    public static ACLProvider getACLProvider(DrillConfig config, String clusterId, String zkRoot) {
        if (config.getBoolean(ExecConstants.ZK_SECURE_ACL)) {
            if (config.getBoolean(ExecConstants.USER_AUTHENTICATION_ENABLED)){
                logger.trace("ZKACLProviderFactory: Using secure ZK ACL");
                return new ZKSecureACLProvider(clusterId, zkRoot);
            } else {
                logger.warn("ZKACLProviderFactory : Secure ZK ACL enabled but user authentication is disabled." +
                            " User authentication is required for secure ZK ACL. Using default un-secure ACL.");
            }
        }
        logger.trace("ZKACLProviderFactory: Using un-secure default ZK ACL");
        return new DefaultACLProvider();
    }
}
