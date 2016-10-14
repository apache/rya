package org.apache.rya.indexing.accumulo.freetext;

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



import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.openrdf.model.Statement;

import org.apache.rya.indexing.StatementSerializer;

/**
 * Row ID: shardId
 * <p>
 * CF: CF Prefix + Term
 */
public class ColumnPrefixes {
	public static final Text DOCS_CF_PREFIX = new Text("d\0");
	public static final Text TERM_CF_PREFIX = new Text("t\0");
	public static final Text TERM_LIST_CF_PREFIX = new Text("l\0");
	public static final Text REVERSE_TERM_LIST_CF_PREFIX = new Text("r\0");

	public static final Text SUBJECT_CF_PREFIX = new Text("s\0");
	public static final Text PREDICATE_CF_PREFIX = new Text("p\0");
	public static final Text OBJECT_CF_PREFIX = new Text("o\0");
	public static final Text CONTEXT_CF_PREFIX = new Text("c\0");

	private static Text concat(Text prefix, String str) {
		Text temp = new Text(prefix);

		try {
			ByteBuffer buffer = Text.encode(str, false);
			temp.append(buffer.array(), 0, buffer.limit());
		} catch (CharacterCodingException cce) {
			throw new IllegalArgumentException(cce);
		}

		return temp;
	}

	public static Text getTermColFam(String term) {
		return concat(TERM_CF_PREFIX, term);
	}

	public static Text getTermListColFam(String term) {
		return concat(TERM_LIST_CF_PREFIX, term);
	}

	public static Text getRevTermListColFam(String term) {
		return concat(REVERSE_TERM_LIST_CF_PREFIX, StringUtils.reverse(term));
	}

	public static Text getDocColFam(String term) {
		return concat(DOCS_CF_PREFIX, term);
	}

	public static Text getSubjColFam(String term) {
		return concat(SUBJECT_CF_PREFIX, term);
	}

	public static Text getSubjColFam(Statement statement) {
		String subj = StatementSerializer.writeSubject(statement);
		return getSubjColFam(subj);
	}

	public static Text getPredColFam(String term) {
		return concat(PREDICATE_CF_PREFIX, term);
	}

	public static Text getPredColFam(Statement statement) {
		String pred = StatementSerializer.writePredicate(statement);
		return getPredColFam(pred);
	}

	public static Text getObjColFam(String term) {
		return concat(OBJECT_CF_PREFIX, term);
	}

	public static Text getObjColFam(Statement statement) {
		String obj = StatementSerializer.writeObject(statement);
		return getObjColFam(obj);
	}

	public static Text getContextColFam(String term) {
		return concat(CONTEXT_CF_PREFIX, term);
	}

	public static Text getContextColFam(Statement statement) {
		String cont = StatementSerializer.writeContext(statement);
		return getContextColFam(cont);
	}

	public static Text removePrefix(Text termWithPrefix) {
		Text temp = new Text();
		temp.set(termWithPrefix.getBytes(), 2, termWithPrefix.getLength() - 2);
		return temp;
	}

}
