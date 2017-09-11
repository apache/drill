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
import org.apache.drill.exec.expr.fn.impl.ContextFunctions;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.eclipse.jetty.security.SpnegoLoginService;
import org.eclipse.jetty.security.SpnegoUserPrincipal;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.B64Code;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import java.lang.reflect.Field;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;

public class DrillSpnegoLoginService extends SpnegoLoginService {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillSpnegoLoginService.class);

    private static final String TARGET_NAME_FIELD_NAME = "_targetName";
    private final String serverPrincipal;
    private final DrillbitContext drillContext;
    private UserIdentity identity;
    private final UserGroupInformation ugi;



    public DrillSpnegoLoginService(SpnegoUtil spnegoUtil, DrillbitContext drillBitContext) throws DrillException {
        super(DrillSpnegoLoginService.class.getName());
        this.serverPrincipal = spnegoUtil.getSpnegoPrincipal();
        drillContext = drillBitContext;
        ugi =  spnegoUtil.getUgi();
    }

    @Override
    protected void doStart() throws Exception {
        // Override the parent implementation, setting _targetName to be the serverPrincipal
        // without the need for a one-line file to do the same thing.
        final Field targetNameField = SpnegoLoginService.class.getDeclaredField(TARGET_NAME_FIELD_NAME);
        targetNameField.setAccessible(true);
        targetNameField.set(this, serverPrincipal);
    }

    @Override
    public UserIdentity login(final String username, final Object credentials) {
        try {
            identity = ugi.doAs(new PrivilegedExceptionAction<UserIdentity>() {
                @Override
                public UserIdentity run() {
                    return spnegologin(username, credentials);
                }
            });
        } catch (Exception e) {
            logger.error("Failed to login using SPNEGO");
        }
        return identity;

    }

    public UserIdentity spnegologin(String username, Object credentials) {

        String encodedAuthToken = (String) credentials;
        byte[] authToken = B64Code.decode(encodedAuthToken);

        GSSManager manager = GSSManager.getInstance();
        try {
            //Providing both OID's is required here. If we provide only one, we're requiring that clients provide us the SPNEGO OID to authentica via Kerberos.
            Oid spnegoOid = new Oid("1.3.6.1.5.5.2");
            Oid krb5Oid = new Oid("1.2.840.113554.1.2.2");
            GSSName gssName = manager.createName(serverPrincipal, null);
            GSSCredential serverCreds = manager.createCredential(gssName,
                    GSSCredential.INDEFINITE_LIFETIME, new Oid[] {krb5Oid, spnegoOid},
                    GSSCredential.ACCEPT_ONLY);
            GSSContext gContext = manager.createContext(serverCreds);
            if (gContext == null) {
                logger.debug("SpnegoUserRealm: failed to establish GSSContext");
            } else {
                while (!gContext.isEstablished()) {
                    authToken = gContext.acceptSecContext(authToken, 0, authToken.length);
                }
                if (gContext.isEstablished()) {
                    String clientName = gContext.getSrcName().toString();
                    String role = clientName.substring(0, clientName.indexOf(64));
                    final SystemOptionManager sysOptions = drillContext.getOptionManager();

                    final boolean isAdmin = ImpersonationUtil.hasAdminPrivileges(role,
                            sysOptions.getOption(ExecConstants.ADMIN_USERS_KEY).string_val,
                            sysOptions.getOption(ExecConstants.ADMIN_USER_GROUPS_KEY).string_val);
                    DrillUserPrincipal user = new DrillUserPrincipal(clientName, isAdmin);
                    Subject subject = new Subject();
                    subject.getPrincipals().add(user);
                    if (isAdmin) {
                        return this._identityService.newUserIdentity(subject, user, DrillUserPrincipal.ADMIN_USER_ROLES);
                    } else {
                        return this._identityService.newUserIdentity(subject, user, DrillUserPrincipal.NON_ADMIN_USER_ROLES);
                    }
                }
            }
        } catch (GSSException gsse) {
            logger.warn("Caught GSSException trying to authenticate the client", gsse);
        }
        return null;
    }
}

