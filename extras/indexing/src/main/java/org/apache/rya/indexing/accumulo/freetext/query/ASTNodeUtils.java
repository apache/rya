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
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

public class ASTNodeUtils {

	/**
	 * Serialize a node (and it's children) to a parsable string.
	 * 
	 * @param s
	 * @return
	 */
	public static String serializeExpression(Node s) {
		if (s instanceof ASTTerm) {
			ASTTerm a = (ASTTerm) s;
			return (a.isNotFlag() ? "!" : "") + " " + a.getTerm();
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
			children.add(serializeExpression(s.jjtGetChild(i)));
		}
		return prefix + StringUtils.join(children, join) + suffix;

	}

	/**
	 * count the number of terms in this query tree.
	 * 
	 * @param node
	 * @return
	 */
	public static int termCount(Node node) {
		if (node instanceof SimpleNode) {
			int count = 0;
			for (SimpleNode n : getNodeIterator((SimpleNode) node)) {
				count += termCount(n);
			}
			return count;
		} else if (node instanceof ASTTerm) {
			return 1;
		} else {
			throw new IllegalArgumentException("Node is of unknown type: " + node.getClass().getName());
		}
	}

	/**
	 * Add the child as the parent's first child.
	 * 
	 * @param parent
	 * @param child
	 */
	public static void pushChild(SimpleNode parent, SimpleNode child) {
		// note: this implementation is very coupled with the SimpleNode jjt implementation
		int parentSize = parent.jjtGetNumChildren();

		// expand the parent node
		parent.jjtAddChild(null, parentSize);

		// get the current head child
		Node currentHeadChild = parent.jjtGetChild(0);

		// set the parameter as the parent's first child
		parent.jjtAddChild(child, 0);

		// add the former head child to the end of the list
		if (currentHeadChild != null) {
			parent.jjtAddChild(currentHeadChild, parentSize);
		}

		// tie the child to the parent
		child.jjtSetParent(parent);

	}

	/**
	 * Get the index of the child, -1 if child not found.
	 * 
	 * @param parent
	 * @param child
	 */
	public static int getChildIndex(SimpleNode parent, SimpleNode child) {
		int parentSize = parent.jjtGetNumChildren();

		for (int i = 0; i < parentSize; i++) {
			if (child.equals(parent.jjtGetChild(i))) {
				return i;
			}
		}

		return -1;
	}

	/**
	 * return true is all of the node's children have the not flag enabled.
	 * 
	 * @param node
	 * @return
	 */
	public static boolean allChildrenAreNot(ASTExpression node) {
		for (SimpleNode child : getNodeIterator(node)) {
			if (!isNotFlag(child)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * return the node's not flag value. node must be of type {@link ASTTerm} or {@link ASTExpression}
	 * 
	 * @param node
	 * @return
	 */
	public static boolean isNotFlag(Node node) {
		if (node instanceof ASTExpression) {
			return ((ASTExpression) node).isNotFlag();
		} else if (node instanceof ASTTerm) {
			return ((ASTTerm) node).isNotFlag();
		} else {
			throw new IllegalArgumentException("Node is of unknown type: " + node.getClass().getName());
		}
	}

	public static Iterable<SimpleNode> getNodeIterator(final SimpleNode n) {
		return new Iterable<SimpleNode>() {

			@Override
			public Iterator<SimpleNode> iterator() {
				return new Iterator<SimpleNode>() {
					int pointer = 0;

					@Override
					public boolean hasNext() {
						return pointer < n.jjtGetNumChildren();
					}

					@Override
					public SimpleNode next() {
						Node rtn = n.jjtGetChild(pointer);
						pointer++;
						return (SimpleNode) rtn;
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}
		};
	}

	public static void swapChildren(ASTExpression parent, int childOneIndex, int childTwoIndex) {
		Validate.isTrue(childOneIndex > -1 && childOneIndex < parent.jjtGetNumChildren());
		Validate.isTrue(childTwoIndex > -1 && childTwoIndex < parent.jjtGetNumChildren());

		Node childOne = parent.jjtGetChild(childOneIndex);
		Node childTwo = parent.jjtGetChild(childTwoIndex);
		parent.jjtAddChild(childOne, childTwoIndex);
		parent.jjtAddChild(childTwo, childOneIndex);
	}

	public static int findFirstNonNotChild(ASTExpression expression) {
		for (int i = 0; i < expression.jjtGetNumChildren(); i++) {
			if (!isNotFlag(expression.jjtGetChild(i))) {
				return i;
			}
		}
		return -1;
	}

}
