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
package org.apache.drill.exec.server.options;

import java.util.Set;

import javax.validation.ConstraintViolation;

import org.apache.drill.common.exceptions.LogicalPlanParsingException;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.LogicalOperatorBase;

public class SetOptionException extends LogicalPlanParsingException{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SetOptionException.class);

  public SetOptionException() {
    super();

  }

  public SetOptionException(LogicalOperator operator, Set<ConstraintViolation<LogicalOperatorBase>> violations) {
    super(operator, violations);

  }

  public SetOptionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);

  }

  public SetOptionException(String message, Throwable cause) {
    super(message, cause);

  }

  public SetOptionException(String message) {
    super(message);

  }

  public SetOptionException(Throwable cause) {
    super(cause);

  }


}
