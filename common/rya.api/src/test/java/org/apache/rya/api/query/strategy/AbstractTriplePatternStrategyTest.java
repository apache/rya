package org.apache.rya.api.query.strategy;

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



import static org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.query.strategy.wholerow.OspWholeRowTriplePatternStrategy;
import org.apache.rya.api.query.strategy.wholerow.PoWholeRowTriplePatternStrategy;
import org.apache.rya.api.query.strategy.wholerow.SpoWholeRowTriplePatternStrategy;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowRegex;
import org.apache.rya.api.resolver.triple.impl.WholeRowTripleResolver;

import org.openrdf.model.vocabulary.XMLSchema;

/**
 * Date: 7/25/12
 * Time: 11:41 AM
 */
public class AbstractTriplePatternStrategyTest extends TestCase {
    public class MockRdfConfiguration extends RdfCloudTripleStoreConfiguration {

		@Override
		public RdfCloudTripleStoreConfiguration clone() {
			return new MockRdfConfiguration();
		}

	}

	public void testRegex() throws Exception {
        RyaURI subj = new RyaURI("urn:test#1234");
        RyaURI pred = new RyaURI("urn:test#pred");
        RyaURI obj = new RyaURI("urn:test#obj");
        RyaStatement ryaStatement = new RyaStatement(subj, pred, obj);
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = new WholeRowTripleResolver().serialize(ryaStatement);
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.SPO);

        String row = new String(tripleRow.getRow());
        TriplePatternStrategy spoStrategy = new SpoWholeRowTriplePatternStrategy();
        TriplePatternStrategy poStrategy = new PoWholeRowTriplePatternStrategy();
        TriplePatternStrategy ospStrategy = new OspWholeRowTriplePatternStrategy();
        //pred
        TripleRowRegex tripleRowRegex = spoStrategy.buildRegex(null, pred.getData(), null, null, null);
        Pattern p = Pattern.compile(tripleRowRegex.getRow());
        Matcher matcher = p.matcher(row);
        assertTrue(matcher.matches());
        //subj
        tripleRowRegex = spoStrategy.buildRegex(subj.getData(), null, null, null, null);
        p = Pattern.compile(tripleRowRegex.getRow());
        matcher = p.matcher(row);
        assertTrue(matcher.matches());
        //obj
        tripleRowRegex = spoStrategy.buildRegex(null, null, obj.getData(), null, null);
        p = Pattern.compile(tripleRowRegex.getRow());
        matcher = p.matcher(row);
        assertTrue(matcher.matches());

        //po table
        row = new String(serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO).getRow());
        tripleRowRegex = poStrategy.buildRegex(null, pred.getData(), null, null, null);
        p = Pattern.compile(tripleRowRegex.getRow());
        matcher = p.matcher(row);
        assertTrue(matcher.matches());

        tripleRowRegex = poStrategy.buildRegex(null, pred.getData(), obj.getData(), null, null);
        p = Pattern.compile(tripleRowRegex.getRow());
        matcher = p.matcher(row);
        assertTrue(matcher.matches());

        tripleRowRegex = poStrategy.buildRegex(subj.getData(), pred.getData(), obj.getData(), null, null);
        p = Pattern.compile(tripleRowRegex.getRow());
        matcher = p.matcher(row);
        assertTrue(matcher.matches());

        //various regex
        tripleRowRegex = poStrategy.buildRegex(null, "urn:test#pr[e|d]{2}", null, null, null);
        p = Pattern.compile(tripleRowRegex.getRow());
        matcher = p.matcher(row);
        assertTrue(matcher.matches());

        //does not match
        tripleRowRegex = poStrategy.buildRegex(null, "hello", null, null, null);
        p = Pattern.compile(tripleRowRegex.getRow());
        matcher = p.matcher(row);
        assertFalse(matcher.matches());
    }

    public void testObjectTypeInfo() throws Exception {
        RyaURI subj = new RyaURI("urn:test#1234");
        RyaURI pred = new RyaURI("urn:test#pred");
        RyaType obj = new RyaType(XMLSchema.LONG, "10");
        RyaStatement ryaStatement = new RyaStatement(subj, pred, obj);
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = RyaTripleContext.getInstance(new MockRdfConfiguration()).serializeTriple(ryaStatement);
        TripleRow tripleRow = serialize.get(SPO);

        String row = new String(tripleRow.getRow());
        TriplePatternStrategy spoStrategy = new SpoWholeRowTriplePatternStrategy();
        //obj
        byte[][] bytes = RyaContext.getInstance().serializeType(obj);
        String objStr = new String(bytes[0]);
        byte[] objectTypeInfo = bytes[1];
        TripleRowRegex tripleRowRegex = spoStrategy.buildRegex(null, null,
                objStr
                , null, objectTypeInfo);
        Pattern p = Pattern.compile(tripleRowRegex.getRow());
        Matcher matcher = p.matcher(row);
        assertTrue(matcher.matches());

        //build row with same object str data
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> dupTriple_str = RyaTripleContext.getInstance(new MockRdfConfiguration()).serializeTriple(
                new RyaStatement(subj, pred, new RyaType(XMLSchema.STRING, objStr))
        );
        TripleRow tripleRow_dup_str = dupTriple_str.get(SPO);

        row = new String(tripleRow_dup_str.getRow());
        spoStrategy = new SpoWholeRowTriplePatternStrategy();

        tripleRowRegex = spoStrategy.buildRegex(null, null,
                objStr
                , null, objectTypeInfo);
        p = Pattern.compile(tripleRowRegex.getRow());
        matcher = p.matcher(row);
        assertFalse(matcher.matches());

        //po table
        TriplePatternStrategy poStrategy = new PoWholeRowTriplePatternStrategy();
        tripleRowRegex = poStrategy.buildRegex(null, null,
                objStr
                , null, objectTypeInfo);
        p = Pattern.compile(tripleRowRegex.getRow());
        String po_row = new String(serialize.get(PO).getRow());
        matcher = p.matcher(po_row);
        assertTrue(matcher.matches());

        tripleRowRegex = poStrategy.buildRegex(null, null,
                objStr
                , null, objectTypeInfo);
        p = Pattern.compile(tripleRowRegex.getRow());
        matcher = p.matcher(new String(dupTriple_str.get(PO).getRow()));
        assertFalse(matcher.matches());

        //osp table
        TriplePatternStrategy ospStrategy = new OspWholeRowTriplePatternStrategy();
        tripleRowRegex = ospStrategy.buildRegex(null, null,
                objStr
                , null, objectTypeInfo);
        p = Pattern.compile(tripleRowRegex.getRow());
        String osp_row = new String(serialize.get(OSP).getRow());
        matcher = p.matcher(osp_row);
        assertTrue(matcher.matches());

        tripleRowRegex = ospStrategy.buildRegex(null, null,
                objStr
                , null, objectTypeInfo);
        p = Pattern.compile(tripleRowRegex.getRow());
        matcher = p.matcher(new String(dupTriple_str.get(OSP).getRow()));
        assertFalse(matcher.matches());
    }
}
