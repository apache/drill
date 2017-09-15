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
package org.apache.drill.exec.server;


import com.google.common.collect.Lists;
import com.typesafe.config.ConfigValueFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.DrillProperties;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.rpc.security.KerberosHelper;
import org.apache.drill.exec.rpc.user.security.TestUserBitKerberos;
import org.apache.drill.exec.rpc.user.security.testing.UserAuthenticatorTestImpl;
import org.apache.drill.exec.server.rest.auth.DrillSecurityHandler;
import org.apache.drill.exec.server.rest.auth.DrillSpnegoAuthenticator;
import org.apache.drill.exec.server.rest.auth.DrillSpnegoLoginService;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.kerby.kerberos.kerb.client.JaasKrbUtil;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.RoleInfo;
import org.eclipse.jetty.security.SecurityHandler;
import org.eclipse.jetty.server.Authentication;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.UserIdentity;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.security.auth.Subject;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.concurrent.Callable;


public class TestSpnegoAuthentication extends BaseTestQuery {

    private static SpnegoTestUtils spnegoHelper;

    @BeforeClass
    public static void setupTest() throws Exception {
         spnegoHelper = new SpnegoTestUtils(TestSpnegoAuthentication.class.getSimpleName());
        spnegoHelper.setupKdc();

        final DrillConfig newConfig = new DrillConfig(DrillConfig.create(cloneDefaultTestConfigProperties())
                .withValue(ExecConstants.HTTP_AUTHENTICATION_MECHANISMS,
                        ConfigValueFactory.fromIterable(Lists.newArrayList("SPNEGO")))
                .withValue(ExecConstants.HTTP_SPNEGO_PRINCIPAL_,
                        ConfigValueFactory.fromAnyRef(spnegoHelper.SERVER_PRINCIPAL))
                .withValue(ExecConstants.HTTP_SPNEGO_KEYTAB_,
                        ConfigValueFactory.fromAnyRef(spnegoHelper.serverKeytab.toString())),
                false);


        final Properties connectionProps = new Properties();
        connectionProps.setProperty(DrillProperties.USER, "anonymous");
        connectionProps.setProperty(DrillProperties.PASSWORD, "anything works!");
        sun.security.krb5.Config.refresh();

        final Field defaultRealm = KerberosName.class.getDeclaredField("defaultRealm");
        defaultRealm.setAccessible(true);
        defaultRealm.set(null, KerberosUtil.getDefaultRealm());

        updateTestCluster(1, newConfig, connectionProps);


    }

    @Test
    public void testRequestWithAuthorization() throws Exception {

        final Properties connectionProps = new Properties();
        connectionProps.setProperty(DrillProperties.SERVICE_PRINCIPAL, spnegoHelper.SERVER_PRINCIPAL);
        connectionProps.setProperty(DrillProperties.KERBEROS_FROM_SUBJECT, "true");
        final Subject clientSubject = JaasKrbUtil.loginUsingKeytab(spnegoHelper.CLIENT_PRINCIPAL,
                spnegoHelper.clientKeytab.getAbsoluteFile());

        String token = Subject.doAs(clientSubject, new PrivilegedExceptionAction<String>() {
            @Override
            public String run() throws Exception {

                GSSManager gssManager = GSSManager.getInstance();
                GSSContext gssContext = null;
                try {
                    String servicePrincipal = spnegoHelper.SERVER_PRINCIPAL;
                    Oid oid = KerberosUtil.getOidInstance("NT_GSS_KRB5_PRINCIPAL");
                    GSSName serviceName = gssManager.createName(servicePrincipal,
                            oid);
                    oid = KerberosUtil.getOidInstance("GSS_KRB5_MECH_OID");
                    gssContext = gssManager.createContext(serviceName, oid, null,
                            GSSContext.DEFAULT_LIFETIME);
                    gssContext.requestCredDeleg(true);
                    gssContext.requestMutualAuth(true);

                    byte[] inToken = new byte[0];
                    byte[] outToken = gssContext.initSecContext(inToken, 0, inToken.length);
                    Base64 base64 = new Base64(0);
                    return base64.encodeToString(outToken);

                } finally {
                    if (gssContext != null) {
                        gssContext.dispose();
                    }
                }
            }
        });

        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);

        Mockito.when(request.getHeader(KerberosAuthenticator.AUTHORIZATION))
                .thenReturn(KerberosAuthenticator.NEGOTIATE + " " + token);
        Mockito.when(request.getServerName()).thenReturn("localhost");



        DrillSpnegoAuthenticator authenticator = new  DrillSpnegoAuthenticator();
        ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
        securityHandler.setAuthenticator(authenticator);
        Authentication authToken = authenticator.validateRequest((ServletRequest)request,(ServletResponse) response,true);



    }


}
