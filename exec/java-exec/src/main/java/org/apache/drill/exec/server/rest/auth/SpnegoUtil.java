/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.drill.exec.server.rest.auth;


import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class SpnegoUtil {

    private final DrillConfig config;
    private UserGroupInformation ugi;
    private String realm;
    private String principal;
    private String keytab;
    public SpnegoUtil(DrillConfig configuration){
        this.config = configuration;
    }


    //Reads the SPNEGO principal from the config file
    public String getSpnegoPrincipal() throws DrillException {
        principal = config.getString(ExecConstants.HTTP_SPNEGO_PRINCIPAL_).trim();
        if(principal.isEmpty()){
            throw new DrillException(" drill.exec.http.spnego.auth.principal in the configuration file can't be empty");
        }
        return principal;
    }

    //Reads the SPNEGO keytab from the config file
    public String getSpnegoKeytab() throws DrillException {
        keytab = config.getString(ExecConstants.HTTP_SPNEGO_KEYTAB_).trim();
        if(keytab.isEmpty()){
            throw new DrillException(" drill.exec.http.spnego.auth.keytab in the configuration file can't be empty");
        }
        return keytab;
    }

    //Performs the Server login to KDC for SPNEGO
    public  UserGroupInformation getUgi() throws DrillException {
        try {
        if (!UserGroupInformation.isSecurityEnabled()) {
            final Configuration newConfig = new Configuration();
            newConfig.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
                    UserGroupInformation.AuthenticationMethod.KERBEROS.toString());
            UserGroupInformation.setConfiguration(newConfig);
            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, this.getSpnegoKeytab());
            final Configuration oldConfig = new Configuration();
            UserGroupInformation.setConfiguration(oldConfig);
        } else {
            ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, this.getSpnegoKeytab());
        }
        }catch (Exception e) {
            throw new DrillException("Login failed for" + principal + "with given keytab");
        }
        return ugi;
    }
}
