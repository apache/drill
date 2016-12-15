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
package org.apache.drill.test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;

import com.google.common.io.Closeables;

/**
 * Compares two files (or file-like sources) to check if they are identical.
 * Used when comparing actual output to a "golden" copy. Provides a line-by-line
 * filter to filter out details that might change from one run to the next
 * (such as user name, the numeric suffix added to generated classes, etc.)
 * <p>
 * Golden files can have internal comments, starting with the '#' character.
 * Use the comments to identify the test that uses the file and other information.
 * <p>
 * Comparing is done line-by-line. Leading and trailing white space is ignored
 * as are blank lines. The resulting non-blank lines are expected to match
 * those in the expected file. (The expected file can also contain white space
 * and blank lines which are also ignored.)
 */

public class FileMatcher {

  public interface Filter {
    String filter(String line);
  }

  /**
   * Generic mechanism for opening a variety of input sources.
   */
  static interface Source {
    Reader reader() throws IOException;
  }

  static class FileSource implements Source {
    File file;

    public FileSource(File file) {
      this.file = file;
    }

    @Override
    public Reader reader() throws IOException {
      return new FileReader(file);
    }
  }

  static class StringSource implements Source {

    String string;

    public StringSource(String string) {
      this.string = string;
    }

    @Override
    public Reader reader() {
      return new StringReader(string);
    }
  }

  static class ReaderSource implements Source {

    Reader reader;

    public ReaderSource(Reader reader) {
      this.reader = reader;
    }

    @Override
    public Reader reader() {
      return reader;
    }
  }

  static class ResourceSource implements Source {

    String resource;

    public ResourceSource(String resource) {
      this.resource = resource;
    }

    @Override
    public Reader reader() {
      try {
        InputStream stream = getClass().getResourceAsStream(resource);
        if (stream == null) {
          throw new IllegalStateException("Resource not found: " + resource);
        }
        return new InputStreamReader(stream, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  /**
   * Creates a FileMatcher given a pair of inputs and other options.
   * One input is the "expected" (or "golden") file that has been pre-filtered.
   * The other is the "actual" value produced by the test. The actual
   * value can be filtered during comparison.
   * <p>
   * The matcher can operate in two modes. Start off with the
   * {@link #capture()} mode to filter the actual input and display it
   * to stdout. When the actual output is correct, copy in into a test
   * resource and remove the call to <tt>capture()</tt>. After that, the
   * matcher will ensure that future actual output matches the expected
   * output.
   */

  public static class Builder {
    private boolean ignoreWhitespace = true;
    private Filter filter;
    private Source expected;
    private Source actual;
    private boolean capture;
    public boolean allowComments = true;

    public Builder expectedFile(File file) {
      expected = new FileSource(file);
      return this;
    }

    public Builder expectedString(String text) {
      expected = new StringSource(text);
      return this;
    }

    public Builder expectedReader(Reader in) {
      expected = new ReaderSource(in);
      return this;
    }

    public Builder expectedResource(String resource) {
      expected = new ResourceSource(resource);
      return this;
    }

    public Builder actualFile(File file) {
      actual = new FileSource(file);
      return this;
    }

    public Builder actualString(String text) {
      actual = new StringSource(text);
      return this;
    }

    public Builder actualReader(Reader in) {
      actual = new ReaderSource(in);
      return this;
    }

    public Builder actualResource(String resource) {
      actual = new ResourceSource(resource);
      return this;
    }

    /**
     * Preserve leading and trailing whitespace. By default,
     * the matcher ignores such spaces.
     *
     * @return
     */

    public Builder preserveWhitespace() {
      ignoreWhitespace = false;
      return this;
    }

    /**
     * By default the matcher ignores comment lines. Call this
     * method to treat comment lines as regular lines.
     * @return
     */

    public Builder disallowComments() {
      allowComments = false;
      return this;
    }

    /**
     * Attach a filter applied to actual lines before comparing the
     * lines with the expected values (or displaying them during
     * capture.)
     *
     * @param filter
     * @return
     */
    public Builder withFilter(Filter filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Call this method to "capture" the expected output (after filtering).
     * Use this when first creating the test to get the text to be used
     * to create the golden file.
     *
     * @return
     */

    public Builder capture() {
      this.capture = true;
      return this;
    }

    /**
     * Performs the matching (or capture) operation.
     */

    public boolean matches() throws IOException {
      FileMatcher matcher = new FileMatcher(this);
      if (capture) {
        matcher.capture();
        return true;
      } else {
        return matcher.match();
      }
    }
  }

  private Builder builder;
  private LineNumberReader expected;
  private LineNumberReader actual;

  private FileMatcher(Builder builder) {
    this.builder = builder;
  }

  public void capture() throws IOException {
    try (LineNumberReader actual = new LineNumberReader(builder.actual.reader())) {
      System.out.println("----- Captured Results -----");
      String line;
      while ((line = actual.readLine()) != null) {
        if (builder.filter != null) {
          line = builder.filter.filter(line);
          if (line == null) {
            continue; }
        }
        System.out.println(line);
      }
      System.out.println("----- End Results -----");
    }
  }

  public boolean match() throws IOException {
    try {
      open();
      return compare();
    }
    finally {
      close();
    }
  }

  private void open() throws IOException {
    expected = new LineNumberReader(builder.expected.reader());
    actual = new LineNumberReader(builder.actual.reader());
  }

  private boolean compare() throws IOException {
    for (; ;) {
      String expectedLine;

      // Find a non-blank expected line.

      for (;;) {
        expectedLine = expected.readLine();
        if (expectedLine == null) {
          break; }
        if (builder.ignoreWhitespace) {
          expectedLine = expectedLine.trim();
          if (expectedLine.isEmpty()) {
            continue; }
        }
        if (builder.allowComments && expectedLine.charAt(0) == '#') {
          continue; }
        break;
      }

      // Find a non-blank actual line (after filtering).

      String actualLine;
      for (; ;) {
        actualLine = actual.readLine();
        if (actualLine == null) {
          break; }
        if (builder.filter != null) {
          actualLine = builder.filter.filter(actualLine);
          if (actualLine == null) {
            continue; }
        }
        if (builder.ignoreWhitespace) {
          actualLine = actualLine.trim();
          if (actualLine.isEmpty()) {
            continue; }
        }
        break;
      }

      // Do the line-by-line comparison.

      if (expectedLine == null  &&  actualLine == null) {
        return true; }
      if (expectedLine == null) {
        System.err.println("Missing lines at line " + expected.getLineNumber());
        return false;
      }
      if (actualLine == null) {
        System.err.println("Unexpected lines at line " + expected.getLineNumber());
        return false;
      }
      if (! actualLine.equals(expectedLine)) {
        System.err.println("Lines differ. Expected: " + expected.getLineNumber() +
                            ", Actual: " + actual.getLineNumber());
        return false;
      }
    }
  }

  private void close() {
    Closeables.closeQuietly(expected);
    Closeables.closeQuietly(actual);
  }
}
