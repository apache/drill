/*
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
package org.apache.drill.exec.server.rest.auth;

import org.apache.drill.common.exceptions.DrillException;
import org.apache.drill.exec.rpc.security.plain.PlainFactory;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.rest.WebServerConstants;
import org.eclipse.jetty.security.authentication.FormAuthenticator;
import org.eclipse.jetty.util.security.Constraint;

public class FormSecurityHanlder extends DrillHttpConstraintSecurityHandler {
  //private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormSecurityHanlder.class);

  @Override
  public String getImplName() {
    return Constraint.__FORM_AUTH;
  }

  @Override
  public void doSetup(DrillbitContext dbContext) throws DrillException {

    // Check if PAMAuthenticator is available or not which is required for FORM authentication
    if (!dbContext.getAuthProvider().containsFactory(PlainFactory.SIMPLE_NAME)) {
      throw new DrillException("FORM mechanism was configured but PLAIN mechanism is not enabled to provide an " +
          "authenticator. Please configure user authentication with PLAIN mechanism and authenticator to use " +
          "FORM authentication");
    }

    setup(new FormAuthenticator(WebServerConstants.FORM_LOGIN_RESOURCE_PATH,
        WebServerConstants.FORM_LOGIN_RESOURCE_PATH, true), new DrillRestLoginService(dbContext));
  }

}