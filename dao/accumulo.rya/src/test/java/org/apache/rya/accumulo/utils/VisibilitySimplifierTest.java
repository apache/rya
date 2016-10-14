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
package org.apache.rya.accumulo.utils;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Tests the methods of {@link VisibilitySimplifier}.
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
}