/**
 * Copyright 2010, BigDataCraft.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.parsers;

import static org.junit.Assert.*;

import java.io.*;

import org.apache.commons.io.FileUtils;
import org.apache.drill.parsers.impl.drqlantlr.Parser;
import org.apache.drill.parsers.utils.ResourceReader;
import org.junit.Test;

public class DrqlParserAstTest {

	File getFile(String filename, String suffix) throws IOException {
		return File.createTempFile(filename, suffix);
	}
	@Test
	public void testQueryList() throws IOException {
	       //tests parsing all SQL that are encountered in the documentation
	       for(int i = 1; i <= 15; i++) {

	           File tempFileParsed = getFile("q"+i+"_temp", "drql.sm");
               File tempFileExpected = getFile("qe"+i+"_tmp", "drql.ast");

               String expectedOutput = ResourceReader.read("q" + i + ".drql.ast");
	           String query = ResourceReader.read("q"+i+".drql");

	           String ast = Parser.parseToAst(query).toStringTree();

               FileUtils.writeStringToFile(tempFileParsed, ast);
               FileUtils.writeStringToFile(tempFileExpected, expectedOutput);

	           assertTrue("sm files differs",
                       FileUtils.contentEquals(tempFileExpected, tempFileParsed));

               FileUtils.forceDelete(tempFileExpected);
               FileUtils.forceDelete(tempFileParsed);
	       }
	}
}
