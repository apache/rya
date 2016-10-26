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
package org.apache.rya.jena.jenasesame;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.sse.SSE;
import org.apache.rya.jena.jenasesame.impl.GraphRepository;
import org.apache.rya.openjena.legacy.atlas.junit.BaseTest;
import org.junit.Test;

/**
 * Basic add and delete tests for a graph
 */
public abstract class AbstractTestGraph2 extends BaseTest {
    private static final Node ANY = Node.ANY;

    // This will become the basis for a general graph test in Jena
    protected static final Node S1 = makeNode("<ex:s1>");
    protected static final Node P1 = makeNode("<ex:p1>");
    protected static final Node O1 = makeNode("<ex:o1>");

    protected static final Node S2 = makeNode("<ex:s2>");
    protected static final Node P2 = makeNode("<ex:p2>");
    protected static final Node O2 = makeNode("<ex:o2>");

    protected static final Node LIT1 = makeNode("'lex'");
    protected static final Node LIT2 = makeNode("'lex'@en");
    protected static final Node LIT3 = makeNode("123");

    private static Triple triple(final Node s, final Node p, final Node o) {
        return new Triple(s, p, o);
    }

    protected abstract Graph emptyGraph();
    protected abstract void returnGraph(Graph g);

    protected static Node makeNode(final String str) {
        return  SSE.parseNode(str);
    }

    @Test
    public void graph_01() {
        final Graph g = emptyGraph();
        assertEquals(0, g.size());
        returnGraph(g);
    }

    @Test
    public void graph_add_01() {
        final Graph g = emptyGraph();
        final Triple t = triple(S1, P1, O1);
        g.add(t);
        assertEquals(1, g.size());
        assertTrue(g.contains(t));
        assertTrue(g.contains(S1, P1, O1));
        returnGraph(g);
    }

    @Test
    public void graph_add_02() {
        final Graph g = emptyGraph();

        final Triple t = triple(S1, P1, O1);
        g.add(t);
        g.add(t);
        assertEquals(1, g.size());
        assertTrue(g.contains(t));
        assertTrue(g.contains(S1, P1, O1));
        returnGraph(g);
    }

    @Test
    public void graph_add_03() {
        final Graph g = emptyGraph();
        // SPO twice -- as different nodes.
        final Node ns1 = makeNode("<ex:s>");
        final Node np1 = makeNode("<ex:p>");
        final Node no1 = makeNode("<ex:o>");

        final Node ns2 = makeNode("<ex:s>");
        final Node np2 = makeNode("<ex:p>");
        final Node no2 = makeNode("<ex:o>");

        final Triple t1 = triple(ns1, np1, no1);
        final Triple t2 = triple(ns2, np2, no2);
        g.add(t1);
        g.add(t2);
        assertEquals(1, g.size());
        assertTrue(g.contains(t1));
        assertTrue(g.contains(t2));
        assertTrue(g.contains(ns1,np1,no1));
        returnGraph(g);
    }

    @Test
    public void graph_add_04() {
        final Graph g = emptyGraph();
        // Literals
        final Triple t1 = triple(S1, P1, LIT1);
        final Triple t2 = triple(S1, P1, LIT2);
        g.add(t1);
        g.add(t2);
        assertEquals(2, g.size());
        assertTrue(g.contains(t1));
        assertTrue(g.contains(t2));
        assertTrue(g.contains(S1, P1, LIT1));
        assertTrue(g.contains(S1, P1, LIT2));
        final Node o = makeNode("<ex:lex>");
        assertFalse(g.contains(S1, P1, o));
        returnGraph(g);
    }

    @Test
    public void graph_add_delete_01() {
        final Graph g = emptyGraph();
        final Triple t = triple(S1, P1, O1);
        g.add(t);
        g.delete(t);
        assertEquals(0, g.size());
        assertFalse("g contains t", g.contains(t));
        returnGraph(g);
    }

    @Test
    public void graph_add_delete_02() {
        final Graph g = emptyGraph();
        final Triple t = triple(S1, P1, O1);
        // reversed from above
        g.delete(t);
        g.add(t);
        assertEquals(1, g.size());
        assertTrue("g does not contain t", g.contains(t));
        returnGraph(g);
    }

    @Test
    public void graph_add_delete_03() {
        final Graph g = emptyGraph();
        final Triple t = triple(S1, P1, O1);
        // Add twice, delete once => empty
        g.add(t);
        g.add(t);
        g.delete(t);
        assertEquals(0, g.size());
        assertFalse("g contains t", g.contains(t));
        returnGraph(g);
    }

    @Test
    public void graph_add_delete_04() {
        final Graph g = emptyGraph();
        final Triple t1 = triple(S1, P1, O1);
        final Triple t2 = triple(S2, P2, O2);

        g.add(t1);
        g.add(t2);
        g.delete(t1);

        assertEquals(1, g.size());
        assertTrue("g does not contain t2", g.contains(t2));
        returnGraph(g);
    }

    @Test
    public void graph_add_find_01() {
        // Tests the "unknown node" handling
        final Graph g = emptyGraph();
        final Triple t1 = triple(S1, P1, O1);
        assertEquals(0, g.size());
        assertFalse(g.contains(t1));
        g.add(t1);
        assertTrue(g.contains(t1));
        returnGraph(g);
    }

    @Test
    public void graph_add_find_02() {
        // Tests the "unknown node" handling
        final Graph g = emptyGraph();
        final Triple t1 = triple(S1, P1, O1);
        assertEquals(0, g.size());
        assertFalse(g.contains(t1));
        g.add(t1);
        assertTrue(g.contains(t1));
        returnGraph(g);
    }

    @Test
    public void remove_01() {
        final Graph g = emptyGraph();
        final Triple t1 = triple(S1, P1, O1);
        g.add(t1);
        if (g instanceof GraphRepository) {
            ((GraphRepository)g).getBulkUpdateHandler().remove(ANY, ANY, ANY);
        }
        assertEquals(0, g.size());
        returnGraph(g);
    }

    @Test
    public void remove_02() {
        final Graph g = emptyGraph();
        final Triple t1 = triple(S1, P1, O1);
        g.add(t1);
        if (g instanceof GraphRepository) {
            ((GraphRepository)g).getBulkUpdateHandler().remove(S2, ANY, ANY);
        }
        assertEquals(1, g.size());
        assertTrue(g.contains(t1));
        returnGraph(g);
    }

    @Test
    public void remove_03() {
        final Graph g = emptyGraph();
        final Triple t1 = triple(S1, P1, O1);
        g.add(t1);
        if (g instanceof GraphRepository) {
            ((GraphRepository)g).getBulkUpdateHandler().remove(S1, ANY, ANY);
        }
        assertEquals(0, g.size());
        returnGraph(g);
    }

    @Test
    public void removeAll_01() {
        final Graph g = emptyGraph();
        final Triple t1 = triple(S1, P1, O1);
        final Triple t2 = triple(S1, P1, O2);
        final Triple t3 = triple(S2, P1, O1);
        final Triple t4 = triple(S2, P1, O2);
        g.add(t1);
        g.add(t2);
        g.add(t3);
        g.add(t4);
        if (g instanceof GraphRepository) {
            ((GraphRepository)g).getBulkUpdateHandler().removeAll();
        }
        assertEquals(0, g.size());
        returnGraph(g);
    }

    @Test
    public void count_01() {
        final Graph g = emptyGraph();
        assertEquals(0, g.size());
        final Triple t1 = triple(S1, P1, O1);
        g.add(t1);
        assertEquals(1, g.size());
        returnGraph(g);
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