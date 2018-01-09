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
package org.apache.rya.api.model.visibility;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests the methods of {@link VisibilitySimplifier}.
 * 
 * XXX
 * This class has been copied over because Rya has decided to use the Accumulo
 * implementation of visibilities to control who is able to access what data
 * within a Rya instance. Until we implement an Accumulo agnostic method for
 * handling those visibility expressions, we have chosen to pull the Accumulo
 * code into our API.
 *
 * Copied from accumulo's org.apache.rya.accumulo.utils.VisibilitySimplifierTest
 *   <dependancy>
 *     <groupId>org.apache.rya.accumulo</groupId>
 *     <artifactId>accumulo.rya</artifactId>
 *     <version>3.2.12-incubating-SNAPSHOT</version>
 *   </dependancy>
 */
public class VisibilitySimplifierTest {

    @Test
    public void noneRequired() {
        final String simplified = new VisibilitySimplifier().simplify("u");
        assertEquals("u", simplified);
    }

    @Test
    public void parenthesis() {
        final String simplified = new VisibilitySimplifier().simplify("(u&u)&u");
        assertEquals("u", simplified);
    }

    @Test
    public void manyAnds() {
        final String simplified = new VisibilitySimplifier().simplify("u&u&u");
        assertEquals("u", simplified);
    }

    @Test
    public void complex() {
        final String simplified = new VisibilitySimplifier().simplify("(a|b)|(a|b)|a|b");
        assertEquals("a|b", simplified);
    }

    @Test
    public void unionAndSimplify() {
        final String simplified = new VisibilitySimplifier().unionAndSimplify("u&b", "u");
        assertEquals("b&u", simplified);
    }

    @Test
    public void unionAndSimplify_firstIsEmpty() {
        final String simplified = new VisibilitySimplifier().unionAndSimplify("", "u");
        assertEquals("u", simplified);
    }

    @Test
    public void unionAndSimplify_secondIsEmpty() {
        final String simplified = new VisibilitySimplifier().unionAndSimplify("u", "");
        assertEquals("u", simplified);
    }

    @Test
    public void unionAndSimplify_bothAreEmpty() {
        final String simplified = new VisibilitySimplifier().unionAndSimplify("", "");
        assertEquals("", simplified);
    }
}