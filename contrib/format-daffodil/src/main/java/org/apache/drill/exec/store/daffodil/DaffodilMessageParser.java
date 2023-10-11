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
package org.apache.drill.exec.store.daffodil;


import org.apache.daffodil.japi.DataProcessor;
import org.apache.daffodil.japi.Diagnostic;
import org.apache.daffodil.japi.ParseResult;
import org.apache.daffodil.japi.infoset.InfosetOutputter;
import org.apache.daffodil.japi.io.InputSourceDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

/**
 * DFDL Daffodil Streaming message parser
 * <br/>
 * You construct this providing a DataProcessor obtained from the
 * DaffodilDataProcessorFactory.
 * The DataProcessor contains the compiled DFDL schema, ready to use, as
 * well as whether validation while parsing has been requested.
 * <br/>
 * The DataProcessor object may be shared/reused by multiple threads each of which
 * has its own copy of this class.
 * This object is, however, stateful, and must not be shared by multiple threads.
 * <br/>
 * You must call setInputStream, and setInfosetOutputter before
 * you call parse().
 * The input stream and the InfosetOutputter objects are also private to one thread and are stateful
 * and owned by this object.
 * Once you have called setInputStream, you should view the input stream as the private property of
 * this object.
 * The parse() will invoke the InfosetOutputter's methods to deliver
 * parsed data, and it may optionally create diagnostics (obtained via getDiagnostics)
 * indicating which kind they are via the getIsProcessingError, getIsValidationError.
 * <br/>
 * Note that the InfosetOutputter may be called many times before a processing error is detected,
 * as Daffodil delivers result data incrementally.
 * <br/>
 * Validation errors do not affect the InfosetOutputter output calls, but indicate that data was
 * detected that is invalid.
 * <br/>
 * When parse() returns, the parse has ended and one can check for errors/diagnostics.
 * One can call parse() again if there is still data to consume, which is checked via the
 * isEOF() method.
 * <br/>
 * There are no guarantees about where the input stream is positioned between parse() calls.
 * In particular, it may not be positioned at the start of the next message, as Daffodil may
 * have pre-fetched additional bytes from the input stream which it found are not part of the
 * current infoset, but the next one.
 * The positioning of the input stream may in fact be somewhere in the middle of a byte,
 * as Daffodil does not require messages to be of lengths that are in whole byte units.
 * Hence, once you give the input stream to this object via setInputStream, that input stream is
 * owned privately by this class for ever after.
 */
public class DaffodilMessageParser {

  /**
   * Constructs the parser using a DataProcessor obtained from
   * a DaffodilDataProcessorFactory.
   * @param dp
   */
  DaffodilMessageParser(DataProcessor dp) {
    this.dp = dp;
  }

  /**
   * Provide the input stream from which data is to be parsed.
   * <br/>
   * This input stream is then owned by this object and becomes part of its state.
   * <br/>
   * It is; however, the responsibility of the caller to close this
   * input stream after the completion of all parse calls.
   * In particular, if a parse error is considered fatal, then
   * the caller should close the input stream.
   * There are advanced error-recovery techniques that may attempt to find
   * data that can be parsed later in the data stream.
   * In those cases the input stream would not be closed after a processing error,
   * but such usage is beyond the scope of this javadoc.
   * @param inputStream
   */
  public void setInputStream(InputStream inputStream) {
    dis = new InputSourceDataInputStream(inputStream);
  }

  /**
   * Provides the InfosetOutputter which will be called to deliver
   * the Infoset via calls to its methods.
   * @param outputter
   */
  public void setInfosetOutputter(InfosetOutputter outputter) {
    this.outputter = outputter;
  }

  /**
   * Called to pull messages from the data stream.
   * The message 'Infoset' is delivered by way of calls to the InfosetOutputter's methods.
   * <br/>
   * After calling this, one may call getIsProcessingError, getIsValiationError, isEOF, and
   * getDiagnostics.
   */
  public void parse() {
    if (dis == null)
      throw new IllegalStateException("Input stream must be provided by setInputStream() call.");
    if (outputter == null)
      throw new IllegalStateException("InfosetOutputter must be provided by setInfosetOutputter() call.");

    reset();
    ParseResult res = dp.parse(dis, outputter);
    isProcessingError = res.isProcessingError();
    isValidationError = res.isValidationError();
    diagnostics = res.getDiagnostics();
  }

  /**
   * True if the input stream is known to contain no more data.
   * If the input stream is a true stream, not a file, then temporary unavailability of data
   * may cause this call to block until the stream is closed from the other end, or data becomes
   * available.
   * <br/>
   * False if the input stream is at EOF, and no more data can be obtained.
   * It is an error to call parse() after isEOF has returned true.
   * @return
   */
  public boolean isEOF() {
    return !dis.hasData();
  }

  /**
   * True if the parse() call failed with a processing error.
   * This indicates that the data was not well-formed and could not be
   * parsed successfully.
   * <br/>
   * It is possible for isProcessingError and isValidationError to both be true.
   * @return
   */
  public boolean isProcessingError() { return isProcessingError; }

  /**
   * True if a validation error occurred during parsing.
   * Subsequently to a validation error occurring, parsing may succeed or fail.
   * after the validation error was detected.
   * @return
   */
  public boolean isValidationError() { return isValidationError; }

  /**
   * After a parse() call this returns null or a list of 1 or more diagnostics.
   * <br/>
   * If isProcessingError or isValidationError are true, then this will contain at least 1
   * diagnostic.
   * If both are true this will contain at least 2 diagnostics.
   * @return
   */
  public List<Diagnostic> getDiagnostics() { return diagnostics;  }
  public String getDiagnosticsAsString() {
    String result = diagnostics.stream()
        .map(Diagnostic::getMessage)
        .collect(Collectors.joining("\n"));
    return result;
  }


  private static final Logger logger = LoggerFactory.getLogger(DaffodilMessageParser.class);

  private List<Diagnostic> diagnostics; // diagnostics.
  private boolean isProcessingError;
  private boolean isValidationError;

  private InputSourceDataInputStream dis;
  private InfosetOutputter outputter;
  private DataProcessor dp;

  private void reset() {
    outputter.reset();
    isProcessingError = false;
    isValidationError = false;
    diagnostics = null;
  }
}
