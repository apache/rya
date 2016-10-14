package org.apache.rya.indexing.accumulo;

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



import junit.framework.Assert;
import org.apache.rya.indexing.StatementSerializer;

import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

public class StatementSerializerTest {

    @Test
    public void testSimpleStatementObjectUri() throws Exception {
        ValueFactory vf = new ValueFactoryImpl();
        Statement s;

        s = new StatementImpl(vf.createURI("foo:subject"), vf.createURI("foo:predicate"), vf.createURI("foo:object"));
        Assert.assertEquals(s, StatementSerializer.readStatement(StatementSerializer.writeStatement(s)));

        s = new ContextStatementImpl(vf.createURI("foo:subject"), vf.createURI("foo:predicate"), vf.createURI("foo:object"),
                vf.createURI("foo:context"));
        Assert.assertEquals(s, StatementSerializer.readStatement(StatementSerializer.writeStatement(s)));
    }

    @Test
    public void testSimpleObjectLiteral() throws Exception {
        ValueFactory vf = new ValueFactoryImpl();
        Statement s;
        String str;

        s = new StatementImpl(vf.createURI("foo:subject"), vf.createURI("foo:predicate"), vf.createURI("foo:object"));
        Assert.assertEquals(s, StatementSerializer.readStatement(StatementSerializer.writeStatement(s)));

        str = "Alice Palace";
        s = new StatementImpl(vf.createURI("foo:subject"), vf.createURI("foo:predicate"), vf.createLiteral(str));
        Assert.assertEquals(s, StatementSerializer.readStatement(StatementSerializer.writeStatement(s)));

        s = new StatementImpl(vf.createURI("foo:subject"), vf.createURI("foo:predicate"), vf.createLiteral(str, "en"));
        Assert.assertEquals(s, StatementSerializer.readStatement(StatementSerializer.writeStatement(s)));

        s = new StatementImpl(vf.createURI("foo:subject"), vf.createURI("foo:predicate"), vf.createLiteral(str, vf.createURI("xsd:string")));
        Assert.assertEquals(s, StatementSerializer.readStatement(StatementSerializer.writeStatement(s)));
    }

    @Test
    public void testObjectLiteralWithDataTypeGarbage() throws Exception {
        // test with some garbage in the literal that may throw off the parser
        ValueFactory vf = new ValueFactoryImpl();
        Statement s;
        String str;

        str = "Alice ^^<Palace>\"";
        s = new StatementImpl(vf.createURI("foo:subject"), vf.createURI("foo:predicate"), vf.createLiteral(str));
        Assert.assertEquals(s, StatementSerializer.readStatement(StatementSerializer.writeStatement(s)));

        s = new StatementImpl(vf.createURI("foo:subject"), vf.createURI("foo:predicate"), vf.createLiteral(str, "en"));
        Assert.assertEquals(s, StatementSerializer.readStatement(StatementSerializer.writeStatement(s)));

        s = new StatementImpl(vf.createURI("foo:subject"), vf.createURI("foo:predicate"), vf.createLiteral(str, vf.createURI("xsd:string")));
        Assert.assertEquals(s, StatementSerializer.readStatement(StatementSerializer.writeStatement(s)));

    }

    @Test
    public void testObjectLiteralWithAtSignGarbage() throws Exception {
        // test with some garbage in the literal that may throw off the parser
        ValueFactory vf = new ValueFactoryImpl();
        Statement s;
        String str;

        str = "Alice @en";
        s = new StatementImpl(vf.createURI("foo:subject"), vf.createURI("foo:predicate"), vf.createLiteral(str));
        Assert.assertEquals(s, StatementSerializer.readStatement(StatementSerializer.writeStatement(s)));

        s = new StatementImpl(vf.createURI("foo:subject"), vf.createURI("foo:predicate"), vf.createLiteral(str, "en"));
        Assert.assertEquals(s, StatementSerializer.readStatement(StatementSerializer.writeStatement(s)));

        s = new StatementImpl(vf.createURI("foo:subject"), vf.createURI("foo:predicate"), vf.createLiteral(str, vf.createURI("xsd:string")));
        Assert.assertEquals(s, StatementSerializer.readStatement(StatementSerializer.writeStatement(s)));
    }

}
