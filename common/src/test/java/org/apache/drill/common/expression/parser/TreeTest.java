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
package org.apache.drill.common.expression.parser;

import java.io.File;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.DOTTreeGenerator;
import org.antlr.stringtemplate.StringTemplate;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.parser.ExprParser.parse_return;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TreeTest {
  public static void main(String[] args) throws Exception {

    ExprLexer lexer = new ExprLexer(
        new ANTLRStringStream(
            "if ($F1) then case when (_MAP.R_NAME = 'AFRICA') then 2 else 4 end else if(4==3) then 1 else if(x==3) then 7 else (if(2==1) then 6 else 4 end) end"));
    // ExprLexer lexer = new ExprLexer(new
    // ANTLRStringStream("if ('blue.red') then 'orange' else if (false) then 1 else 0 end"));
    // ExprLexer lexer = new ExprLexer(new ANTLRStringStream("2+2"));

    CommonTokenStream tokens = new CommonTokenStream(lexer);

    ExprParser parser = new ExprParser(tokens);
    parse_return ret = parser.parse();
    LogicalExpression e = ret.e;
    ObjectMapper mapper = new ObjectMapper();
    mapper.enable(SerializationFeature.INDENT_OUTPUT);
    System.out.println(mapper.writeValueAsString(e));

    // print the tree
    CommonTree tree = (CommonTree) ret.getTree();
    DOTTreeGenerator gen = new DOTTreeGenerator();
    StringTemplate st = gen.toDOT(tree);

    Files.write(st.toString(), new File("/Users/jnadeau/Documents/tree.dot"), Charsets.UTF_8);

  }
}
