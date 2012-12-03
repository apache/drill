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
import java.util.Formatter;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.antlr.runtime.tree.Tree;
import org.apache.commons.io.FileUtils;
import org.apache.drill.parsers.impl.drqlantlr.AntlrParser;
import org.apache.drill.parsers.impl.drqlantlr.AstNode;
import org.junit.Test;


public class DrqlParserAstTest {

	File getFile(String filename) {
		return new File("testdata" + File.separatorChar + filename);
	}
	@Test
	public void testQueryList() throws IOException {
        Joiner withNewline = Joiner.on("\n");
        //tests parsing all SQL that are encountered in the documentation
        for(int i = 1; i <= 15; i++) {

            File tempFile = getFile("q" + i + "_temp.drql.sm");
            File expectedFile = getFile("q" + i + ".drql.ast");
            File queryFile = getFile("q" + i + ".drql");

            String query = FileUtils.readFileToString(queryFile);
            String ast = AntlrParser.parseToAst(query).toStringTree();
            Formatter f = new Formatter();
            formatAst(AntlrParser.parseToAst(query), f, 0);

            query = withNewline.join(Resources.readLines(Resources.getResource("q" + i + ".drql"), Charsets.UTF_8));
            assertEquals(query, f.toString());

            FileUtils.writeStringToFile(tempFile, ast);

               assertEquals(withNewline.join(Files.readLines(expectedFile, Charsets.UTF_8)), withNewline.join(Files.readLines(expectedFile, Charsets.UTF_8)));
               assertEquals(String.format("sm files differs %s versus %s",expectedFile, tempFile),
                       withNewline.join(Files.readLines(expectedFile, Charsets.UTF_8)),
                       withNewline.join(Files.readLines(tempFile, Charsets.UTF_8)));
               assertTrue(String.format("sm files differs %s versus %s",expectedFile, tempFile),
                       FileUtils.contentEquals(expectedFile, tempFile));

               FileUtils.forceDelete(tempFile);
           }
	}


    public void formatAst(AstNode astNode, Formatter formatter, int indent) {
        String spaces = "                                                    ";
        formatter.format("%s%s\n", spaces.substring(0, indent), astNode.getText());
        int n = astNode.getChildCount();
        for (int i = 0; i < n; i++) {
            formatAst(astNode.getChild(i), formatter, indent + 2);
        }
    }

    public void formatAst(Tree astNode, Formatter formatter, int indent) {
        String spaces = "                                                    ";
        formatter.format("%s%s\n", spaces.substring(0, indent), astNode.getText());
        int n = astNode.getChildCount();
        for (int i = 0; i < n; i++) {
            formatAst(astNode.getChild(i), formatter, indent + 2);
        }
    }
}
