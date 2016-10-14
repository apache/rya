package org.apache.rya.indexing.accumulo.freetext.query;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */



import java.util.ArrayList;
import java.util.List;

import org.apache.rya.indexing.accumulo.freetext.query.ASTExpression;
import org.apache.rya.indexing.accumulo.freetext.query.ASTTerm;
import org.apache.rya.indexing.accumulo.freetext.query.Node;
import org.apache.rya.indexing.accumulo.freetext.query.ParseException;
import org.apache.rya.indexing.accumulo.freetext.query.QueryParser;
import org.apache.rya.indexing.accumulo.freetext.query.TokenMgrError;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class QueryParserTest {

	@Test
	public void AssortmentTest() throws Exception {
		runTest("a* or b", //
				"([WILDTERM]a* OR [TERM]b)");

		runTest("a and b", //
				"([TERM]a AND [TERM]b)");

		runTest("a b", //
				"([TERM]a AND [TERM]b)");

		runTest("a b c", //
				"([TERM]a AND [TERM]b AND [TERM]c)");
		
		runTest("(a and b)", //
				"([TERM]a AND [TERM]b)");

		runTest("(a and b) and c", //
				"(([TERM]a AND [TERM]b) AND [TERM]c)");

		runTest("alpha and beta or charlie and delta or (boo and par)", //
				"(([TERM]alpha AND [TERM]beta) OR ([TERM]charlie AND [TERM]delta) OR ([TERM]boo AND [TERM]par))");

		runTest("a and (b or c)", //
				"([TERM]a AND ([TERM]b OR [TERM]c))");

		runTest("not a and (b or c)", //
				"(![TERM]a AND ([TERM]b OR [TERM]c))");

		runTest("not a and not (b or c)", //
				"(![TERM]a AND !([TERM]b OR [TERM]c))");

		runTest("not a and not (b or \"c and d\")", //
				"(![TERM]a AND !([TERM]b OR [QUOTED]\"c and d\"))");

		runTest("((a and b) and c)", //
				"(([TERM]a AND [TERM]b) AND [TERM]c)");

		runTest("not(a and b)", //
				"!([TERM]a AND [TERM]b)");

		runTest("not(not(a and b))", //
				"([TERM]a AND [TERM]b)");

		runTest("(not(!a and b))", //
				"!(![TERM]a AND [TERM]b)");

		runTest("not(!a and b)", //
				"!(![TERM]a AND [TERM]b)");

		runTest("not a", //
				"![TERM]a");

		runTest("not(not a)", //
				"[TERM]a");

		runTest("(not(!A or B))", //
				"!(![TERM]A OR [TERM]B)");

		runTest("not \"!A\"", //
				"![QUOTED]\"!A\"");
}

	private static void runTest(String query, String expected) throws ParseException, TokenMgrError {
		Assert.assertEquals(expected, prettyPrint(QueryParser.parse(query)));
	}

	public static String prettyPrint(Node s) {
		if (s instanceof ASTTerm) {
			ASTTerm a = (ASTTerm) s;
			return (a.isNotFlag() ? "!" : "") + "[" + a.getType() + "]" + a.getTerm();
		}

		String prefix = "";
		String suffix = "";
		String join = " ";
		if (s instanceof ASTExpression) {
			ASTExpression a = (ASTExpression) s;
			prefix = (a.isNotFlag() ? "!" : "") + "(";
			suffix = ")";
			join = " " + a.getType() + " ";
		}

		List<String> children = new ArrayList<String>();
		for (int i = 0; i < s.jjtGetNumChildren(); i++) {
			children.add(prettyPrint(s.jjtGetChild(i)));
		}
		return prefix + StringUtils.join(children, join) + suffix;

	}
}
