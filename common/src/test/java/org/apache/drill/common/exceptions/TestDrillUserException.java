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
package org.apache.drill.common.exceptions;

import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test various use cases around creating user exceptions
 */
public class TestDrillUserException {

  private Exception wrap(DrillUserException uex, int numWraps) {
    Exception ex = uex;
    for (int i = 0; i < numWraps; i++) {
      ex = new Exception("wrap #" + (i+1), ex);
    }

    return ex;
  }

  // make sure system exceptions are created properly
  @Test
  public void testBuildSystemException() {
    try {
      throw new DrillUserException.Builder(new RuntimeException("this is an exception")).build();
    } catch (DrillUserException ex) {
      DrillPBError error = ex.getOrCreatePBError(true);
      Assert.assertEquals(ErrorType.SYSTEM, error.getErrorType());
    }
  }

  @Test
  public void testBuildUserExceptionWithMessage() {
    String message = "Test message";

    DrillUserException uex = new DrillUserException.Builder(ErrorType.DATA_WRITE, message).build();
    DrillPBError error = uex.getOrCreatePBError(false);

    Assert.assertEquals(ErrorType.DATA_WRITE, error.getErrorType());
    Assert.assertEquals(message, uex.getOriginalMessage());
  }

  @Test
  public void testBuildUserExceptionWithCause() {
    String message = "Test message";

    DrillUserException uex = new DrillUserException.Builder(ErrorType.DATA_WRITE, new RuntimeException(message)).build();
    DrillPBError error = uex.getOrCreatePBError(false);

    // cause message should be used
    Assert.assertEquals(ErrorType.DATA_WRITE, error.getErrorType());
    Assert.assertEquals(message, uex.getOriginalMessage());
  }

  @Test
  public void testBuildUserExceptionWithCauseAndMessage() {
    String messageA = "Test message A";
    String messageB = "Test message B";

    DrillUserException uex = new DrillUserException.Builder(ErrorType.DATA_WRITE, new RuntimeException(messageA), messageB).build();
    DrillPBError error = uex.getOrCreatePBError(false);

    // passed message should override the cause message
    Assert.assertEquals(ErrorType.DATA_WRITE, error.getErrorType());
    Assert.assertFalse(error.getMessage().contains(messageA)); // messageA should not be part of the context
    Assert.assertEquals(messageB, uex.getOriginalMessage());
  }

  @Test
  public void testBuildUserExceptionWithUserExceptionCauseAndMessage() {
    String messageA = "Test message A";
    String messageB = "Test message B";

    DrillUserException original = new DrillUserException.Builder(ErrorType.CONNECTION, messageA).build();
    DrillUserException uex = new DrillUserException.Builder(ErrorType.DATA_WRITE, wrap(original, 5), messageB).build();

    //builder should return the unwrapped original user exception and not build a new one
    Assert.assertEquals(original, uex);

    DrillPBError error = uex.getOrCreatePBError(false);
    Assert.assertEquals(messageA, uex.getOriginalMessage());
    Assert.assertTrue(error.getMessage().contains(messageB)); // messageA should be part of the context
  }

  @Test
  public void testBuildUserExceptionWithFormattedMessage() {
    String format = "This is test #%d";

    DrillUserException uex = new DrillUserException.Builder(ErrorType.CONNECTION, format, 5).build();
    DrillPBError error = uex.getOrCreatePBError(false);

    Assert.assertEquals(ErrorType.CONNECTION, error.getErrorType());
    Assert.assertEquals(String.format(format, 5), uex.getOriginalMessage());
  }

  // make sure wrapped user exceptions are retrieved properly when calling ErrorHelper.wrap()
  @Test
  public void testWrapUserException() {
    DrillUserException uex = new DrillUserException.Builder(ErrorType.DATA_READ, "this is a data read exception").build();

    Exception wrapped = wrap(uex, 3);
    Assert.assertEquals(uex, ErrorHelper.wrap(wrapped));
  }

  @Test
  public void testEdgeCases() {
    new DrillUserException.Builder(null).build();
    new DrillUserException.Builder(ErrorType.DATA_WRITE, null).build().getOrCreatePBError(true);
    new DrillUserException.Builder(ErrorType.DATA_WRITE, null).build().getOrCreatePBError(true);
    new DrillUserException.Builder(ErrorType.DATA_WRITE, new RuntimeException(), null).build().getOrCreatePBError(true);
  }
}
