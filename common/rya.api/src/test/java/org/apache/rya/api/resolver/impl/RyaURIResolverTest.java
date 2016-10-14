package org.apache.rya.api.resolver.impl;

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



import junit.framework.TestCase;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;

/**
 * Date: 7/16/12
 * Time: 2:51 PM
 */
public class RyaURIResolverTest extends TestCase {

    public void testSerialization() throws Exception {
        RyaURI ryaURI = new RyaURI("urn:testdata#data");
        byte[] serialize = new RyaURIResolver().serialize(ryaURI);
        RyaType deserialize = new RyaURIResolver().deserialize(serialize);
        assertEquals(ryaURI, deserialize);
    }
}
