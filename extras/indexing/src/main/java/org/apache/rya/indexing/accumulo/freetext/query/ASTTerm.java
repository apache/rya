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



/**
 * This is a slightly modified version of the ASTTerm file created by JavaCC. This version adds more state to the standard ASTTerm file
 * including a "term", "type", and "notFlag".
 */
public class ASTTerm extends SimpleNode {
	public static final String WILDTERM = "WILDTERM";
	public static final String PREFIXTERM = "PREFIXTERM";
	public static final String QUOTED = "QUOTED";
	public static final String TERM = "TERM";

	private String term = "";
	private boolean notFlag = false;
	private String type = "";

	public ASTTerm(int id) {
		super(id);
	}

	public ASTTerm(QueryParser p, int id) {
		super(p, id);
	}

	@Override
	public String toString() {
		return super.toString() + "[notFlag: " + notFlag + " term: " + term + " type: " + type + "]";
	}

	@Override
	public String toString(String prefix) {
		return super.toString(prefix);
	}

	public String getTerm() {
		return term;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	public boolean isNotFlag() {
		return notFlag;
	}

	public void setNotFlag(boolean notFlag) {
		this.notFlag = notFlag;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}
}
