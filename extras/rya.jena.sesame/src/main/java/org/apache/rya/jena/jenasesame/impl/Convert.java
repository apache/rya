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
/*
 * (c) Copyright 2009 Talis Information Ltd.
 * (c) Copyright 2010 Epimorphics Ltd.
 * All rights reserved.
 * [See end of file]
 */
package org.apache.rya.jena.jenasesame.impl;

import org.apache.commons.lang.StringUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

/**
 * Utility methods to convert nodes and values.
 */
final class Convert {
    /**
     * Private constructor to prevent instantiation.
     */
    private Convert() {
    }

    /**
     * Converts a value to a node.
     * @param value the {@link Value} to convert.
     * @return the {@link Node}.
     */
    public static Node valueToNode(final Value value) {
        if (value instanceof Literal) {
            return literalToNode((Literal)value);
        } else if (value instanceof URI) {
            return uriToNode((URI)value);
        } else if (value instanceof BNode) {
            return bnodeToNode((BNode)value);
        } else {
            throw new IllegalArgumentException("Not a concrete value");
        }
    }

    /**
     * Converts a bnode to a node.
     * @param bnode the {@link BNode} to convert.
     * @return the {@link Node}.
     */
    public static Node bnodeToNode(final BNode bnode) {
        return NodeFactory.createBlankNode(bnode.getID());
    }

    /**
     * Converts a URI to a node.
     * @param uri the {@link URI} to convert}.
     * @return the {@link Node}.
     */
    public static Node uriToNode(final URI uri) {
        return NodeFactory.createURI(uri.stringValue());
    }

    /**
     * Converts a literal to a node.
     * @param literal the {@link Literal} to convert.
     * @return the {@link Node}.
     */
    public static Node literalToNode(final Literal literal) {
        if (literal.getLanguage() != null) {
            return NodeFactory.createLiteral(literal.getLabel(), literal.getLanguage(), false);
        } else if (literal.getDatatype() != null) {
            return NodeFactory.createLiteral(literal.getLabel(), null, NodeFactory.getType(literal.getDatatype().stringValue()));
        } else {
            // Plain literal
            return NodeFactory.createLiteral(literal.getLabel());
        }
    }

    /**
     * Converts a statement to a triple.
     * @param statement the {@link Statement} to convert.
     * @return the {@link Triple}.
     */
    public static Triple statementToTriple(final Statement statement) {
        final Node s = Convert.valueToNode(statement.getSubject());
        final Node p = Convert.uriToNode(statement.getPredicate());
        final Node o = Convert.valueToNode(statement.getObject());
        return new Triple(s, p, o);
    }

    /**
     * Converts a node to a value.
     * @param factory the {@link ValueFactory} to use.
     * @param node the {@link Node}.
     * @return the {@link Value}.
     */
    public static Value nodeToValue(final ValueFactory factory, final Node node) {
        if (node.isLiteral()) {
            return nodeToLiteral(factory, node);
        } else if (node.isURI()) {
            return nodeToURI(factory, node);
        } else if (node.isBlank()) {
            return nodeToBNode(factory, node);
        } else {
            throw new IllegalArgumentException("Not a concrete node");
        }
    }

    /**
     * Converts a node to a resource.
     * @param factory the {@link ValueFactory} to use.
     * @param node the {@link Node}.
     * @return the {@link Resource}.
     */
    public static Resource nodeToResource(final ValueFactory factory, final Node node) {
        if (node.isURI()) {
            return nodeToURI(factory, node);
        } else if (node.isBlank()) {
            return nodeToBNode(factory, node);
        } else {
            throw new IllegalArgumentException("Not a URI nor a blank node");
        }
    }

    /**
     * Converts a node to a bnode.
     * @param factory the {@link ValueFactory}.
     * @param node the {@link Node}.
     * @return the {@link BNode}.
     */
    public static BNode nodeToBNode(final ValueFactory factory, final Node node) {
        return factory.createBNode(node.getBlankNodeLabel());
    }

    /**
     * Converts a node to a URI.
     * @param factory the {@link ValueFactory}.
     * @param node the {@link Node}.
     * @return the {@link URI}.
     */
    public static URI nodeToURI(final ValueFactory factory, final Node node) {
        return factory.createURI(node.getURI());
    }

    /**
     * Converts a node to a literal.
     * @param factory the {@link ValueFactory}.
     * @param node the {@link Node}.
     * @return the {@link Literal}
     */
    public static Literal nodeToLiteral(final ValueFactory factory, final Node node) {
        if (node.getLiteralDatatype() != null) {
            final URI datatypeUri = factory.createURI(node.getLiteralDatatypeURI());
            return factory.createLiteral(node.getLiteralLexicalForm(), datatypeUri);
        } else if (StringUtils.isNotEmpty(node.getLiteralLanguage())) {
            return factory.createLiteral(node.getLiteralLexicalForm(), node.getLiteralLanguage());
        } else {
            return factory.createLiteral(node.getLiteralLexicalForm());
        }
    }
}

/*
 * (c) Copyright 2009 Talis Information Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */