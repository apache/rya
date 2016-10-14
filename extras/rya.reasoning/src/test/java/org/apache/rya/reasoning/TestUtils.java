package org.apache.rya.reasoning;

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

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaURI;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;

public class TestUtils {
    private static final ValueFactory VALUE_FACTORY = new ValueFactoryImpl();
    public static final String TEST_PREFIX = "http://test.test";
    public static final URI NODE = uri("http://thisnode.test", "x");

    public static URI uri(String prefix, String u) {
        if (prefix.length() > 0) {
            u = prefix + "#" + u;
        }
        return VALUE_FACTORY.createURI(u);
    }

    public static URI uri(String u) {
        return uri(TEST_PREFIX, u);
    }

    public static Fact fact(Resource s, URI p, Value o) {
        return new Fact(s, p, o);
    }

    public static Statement statement(Resource s, URI p, Value o) {
        return VALUE_FACTORY.createStatement(s, p, o);
    }

    public static Literal intLiteral(String s) {
        return VALUE_FACTORY.createLiteral(s, XMLSchema.INT);
    }

    public static Literal stringLiteral(String s) {
        return VALUE_FACTORY.createLiteral(s, XMLSchema.STRING);
    }

    public static Literal stringLiteral(String s, String lang) {
        return VALUE_FACTORY.createLiteral(s, lang);
    }

    public static BNode bnode(String id) {
        return VALUE_FACTORY.createBNode(id);
    }

    public static RyaStatement ryaStatement(String s, String p, String o) {
        return new RyaStatement(new RyaURI(TEST_PREFIX + "#" + s),
            new RyaURI(TEST_PREFIX + "#" + p), new RyaURI(TEST_PREFIX + "#" + o));
    }
}
