/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.plan;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.antlr.runtime.ANTLRReaderStream;
import org.antlr.runtime.MissingTokenException;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.apache.drill.plan.ast.LogicalPlanParseException;
import org.apache.drill.plan.ast.Plan;
import org.apache.drill.plan.ast.PlanLexer;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

import static junit.framework.Assert.*;

public class ParsePlanTest {
    @Test
    public void testParse1() throws IOException, RecognitionException, ParsePlan.ValidationException {
        Plan r = ParsePlan.parseResource("plan1.drillx");
        assertEquals("Lines", 3, r.getStatements().size());
    }

    @Test
    public void testParse2() throws IOException, RecognitionException, ParsePlan.ValidationException {
        Plan r = ParsePlan.parseResource("plan2.drillx");
        assertEquals("Lines", 6, r.getStatements().size());
    }

    @Test
    public void testParse3() throws IOException, RecognitionException, ParsePlan.ValidationException {
        Plan r = ParsePlan.parseResource("plan3.drillx");
        assertEquals("Lines", 8, r.getStatements().size());
    }

    @Test
    public void testParseError1() throws IOException, RecognitionException, ParsePlan.ValidationException {
        try {
            ParsePlan.parseResource("bad-plan1.drillx");
            fail("Should have thrown exception");
        } catch (ParsePlan.ValidationException e) {
            assertTrue(e.getMessage().contains("%2 used more than once"));
            assertTrue(e.getMessage().contains("Undefined reference to %3"));
        }
    }

    @Test
    public void testParseError2() throws IOException, RecognitionException, ParsePlan.ValidationException {
        try {
            ParsePlan.parseResource("bad-plan2.drillx");
            fail("Should have thrown exception");
        } catch (LogicalPlanParseException e) {
            assertTrue(e.getCause() instanceof MissingTokenException);
            MissingTokenException ex = ((MissingTokenException) e.getCause());
            assertEquals(1, ex.line);
            assertEquals(6, ex.charPositionInLine);
        }
    }



    @Test
    public void testLexer() throws IOException {
        List<String> ref = Lists.newArrayList(
                "%3", ",", "%4", ":=", "explode", "\"data\"", ",", "\"var-to-explode\"", "\n",
                "%5", ":=", "modify", "%4", "\n",
                "%6", ",", "%7", ":=", "flatten", "%3", ",", "%5", "\n");

        InputStreamReader inStream = Resources.newReaderSupplier(Resources.getResource("plan1.drillx"), Charsets.UTF_8).getInput();
        PlanLexer lex = new PlanLexer(new ANTLRReaderStream(inStream));
        Token t = lex.nextToken();
        Iterator<String> i = ref.iterator();
        while (t != null && t.getType() != -1) {
            if (t.getChannel() != 99) {
                assertEquals(i.next(), t.getText());
            }
            t = lex.nextToken();
        }
        inStream.close();

    }

    @Test
    public void testLexer2() throws IOException {
        List<String> ref = Lists.newArrayList(
                "%1", ":=", "scan-json", "\"table-1\"", "EOL",
                "EOL",
                "%2", ":=", "bind", "\"x\"", ",", "%1", "EOL",
                "EOL",
                "EOL",
                "%3", ":=", "bind", "\"y\"", ",", "%2", "EOL",
                "%4", ":=", ">", "%2", ",", "3", "EOL",
                "%5", ":=", "filter", "%4", ",", "%1", "EOL",
                "%6", ":=", "project", "%5", ",", "%2", ",", "%3", "EOL");

        InputStreamReader inStream = Resources.newReaderSupplier(Resources.getResource("plan2.drillx"), Charsets.UTF_8).getInput();
        PlanLexer lex = new PlanLexer(new ANTLRReaderStream(inStream));
        Token t = lex.nextToken();
        Iterator<String> i = ref.iterator();
        while (t != null && t.getType() != -1) {
            if (t.getChannel() != 99) {
                String tokenText = t.getText();
                if (t.getText().equals("\n")) {
                    tokenText = "EOL";
                }
                assertEquals(i.next(), tokenText);
            }
            t = lex.nextToken();
        }
        inStream.close();

    }
}
