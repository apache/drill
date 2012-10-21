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
import org.apache.drill.parsers.impl.drqlantlr.AstNode;
import org.apache.drill.parsers.impl.drqlantlr.Parser;
import org.apache.drill.parsers.impl.drqlantlr.SemanticModel;
import org.junit.Test;

public class DrqlParserAstTest {

	File getFile(String filename) {
		return new File("testdata" + File.separatorChar + filename);
	}
	@Test
	public void testQueryList() throws IOException {
	       //tests parsing all SQL that are encountered in the documentation
	       for(int i = 1; i <= 15; i++) {

	           File tempFile = getFile("q"+i+"_temp.drql.sm");
	           File expectedFile = getFile("q"+i+".drql.ast");
	           File queryFile = getFile("q"+i+".drql");
	           
	           String query = FileUtils.readFileToString(queryFile);
	           String ast = Parser.parseToAst(query).toStringTree();
	           
	           FileUtils.writeStringToFile(tempFile, ast);

	           assertTrue("sm files differs",
                       FileUtils.contentEquals(expectedFile, tempFile));

	           FileUtils.forceDelete(tempFile);
	       }
	}
}
