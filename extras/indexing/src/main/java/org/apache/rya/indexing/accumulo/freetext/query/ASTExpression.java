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
 * This is a slightly modified version of the ASTExpression file created by JavaCC. This version adds more state to the standard ASTTerm
 * file including a "type", and "notFlag".
 */
public class ASTExpression extends SimpleNode {
	public static final String AND = "AND";
	public static final String OR = "OR";

	private String type = "";
	private boolean notFlag = false;

	public ASTExpression(int id) {
		super(id);
	}

	public ASTExpression(QueryParser p, int id) {
		super(p, id);
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}

	public boolean isNotFlag() {
		return notFlag;
	}

	public void setNotFlag(boolean notFlag) {
		this.notFlag = notFlag;
	}

	@Override
	public String toString() {
		return super.toString() + " [type: " + type + ", notFlag: " + notFlag + "]";
	}
}
