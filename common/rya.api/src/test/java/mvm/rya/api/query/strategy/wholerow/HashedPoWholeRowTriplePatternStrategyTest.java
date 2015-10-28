package mvm.rya.api.query.strategy.wholerow;

/*
 * #%L
 * mvm.rya.rya.api
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import junit.framework.TestCase;
import mvm.rya.api.RdfCloudTripleStoreConstants;
import mvm.rya.api.domain.*;
import mvm.rya.api.query.strategy.ByteRange;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.RyaTripleContext;
import mvm.rya.api.resolver.triple.TripleRow;

import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.openrdf.model.impl.URIImpl;

import java.util.Map;

/**
 * Date: 7/14/12
 * Time: 11:46 AM
 */
public class HashedPoWholeRowTriplePatternStrategyTest extends TestCase {

    RyaURI uri = new RyaURI("urn:test#1234");
    RyaURI uri2 = new RyaURI("urn:test#1235");
    RyaURIRange rangeURI = new RyaURIRange(uri, uri2);
    RyaURIRange rangeURI2 = new RyaURIRange(new RyaURI("urn:test#1235"), new RyaURI("urn:test#1236"));
    HashedPoWholeRowTriplePatternStrategy strategy = new HashedPoWholeRowTriplePatternStrategy();
    RyaContext ryaContext = RyaContext.getInstance();
    RyaTripleContext ryaTripleContext;

    RyaType customType1 = new RyaType(new URIImpl("urn:custom#type"), "1234");
    RyaType customType2 = new RyaType(new URIImpl("urn:custom#type"), "1235");
    RyaType customType3 = new RyaType(new URIImpl("urn:custom#type"), "1236");
    RyaTypeRange customTypeRange1 = new RyaTypeRange(customType1, customType2);
    RyaTypeRange customTypeRange2 = new RyaTypeRange(customType2, customType3);

    @Before
    public void setUp() {
    	MockRdfCloudConfiguration config = new MockRdfCloudConfiguration();
    	config.set(MockRdfCloudConfiguration.CONF_PREFIX_ROW_WITH_HASH, Boolean.TRUE.toString());
    	ryaTripleContext = RyaTripleContext.getInstance(config);
    }

    public void testPoRange() throws Exception {
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaTripleContext.serializeTriple(
                new RyaStatement(uri, uri, uri, null));
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);

        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(null, uri, rangeURI, null, null);
        assertContains(entry.getValue(), tripleRow.getRow());

        entry = strategy.defineRange(null, uri, rangeURI2, null, null);
        assertContainsFalse(entry.getValue(), tripleRow.getRow());
  }

	private void assertContains(ByteRange value, byte[] row) {
	       Text rowText = new Text(row);
	        Text startText = new Text(value.getStart());
	        Text endText = new Text(value.getEnd());
	        assertTrue((startText.compareTo(rowText) <= 0) &&(endText.compareTo(rowText) >= 0)) ;
	}

	private void assertContainsFalse(ByteRange value, byte[] row) {
	       Text rowText = new Text(row);
	        Text startText = new Text(value.getStart());
	        Text endText = new Text(value.getEnd());
	        assertFalse((startText.compareTo(rowText) <= 0) &&(endText.compareTo(rowText) >= 0)) ;
	}

    public void testPoRangeCustomType() throws Exception {
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaTripleContext.serializeTriple(
                new RyaStatement(uri, uri, customType1, null));
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);

        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(null, uri, customTypeRange1, null, null);
        assertContains(entry.getValue(), tripleRow.getRow());

        entry = strategy.defineRange(null, uri, customTypeRange2, null, null);
        assertContainsFalse(entry.getValue(), tripleRow.getRow());
  }

    public void testPo() throws Exception {
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaTripleContext.serializeTriple(
                new RyaStatement(uri, uri, uri, null));
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);

        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(null, uri, uri, null, null);
        assertContains(entry.getValue(), tripleRow.getRow());

        entry = strategy.defineRange(null, uri, uri2, null, null);
        assertContainsFalse(entry.getValue(), tripleRow.getRow());
  }

    public void testPoCustomType() throws Exception {
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaTripleContext.serializeTriple(
                new RyaStatement(uri, uri, customType1, null));
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);

        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(null, uri, customType1, null, null);
        assertContains(entry.getValue(), tripleRow.getRow());

        entry = strategy.defineRange(null, uri, customType2, null, null);
        assertContainsFalse(entry.getValue(), tripleRow.getRow());
    }

    public void testPosRange() throws Exception {
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaTripleContext.serializeTriple(
                new RyaStatement(uri, uri, uri, null));
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);

        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(rangeURI, uri, uri, null, null);
        assertContains(entry.getValue(), tripleRow.getRow());

        entry = strategy.defineRange(rangeURI2, uri, uri, null, null);
        assertContainsFalse(entry.getValue(), tripleRow.getRow());
    }

    public void testPRange() throws Exception {
        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(null, rangeURI, null, null, null);
        assertNull(entry);
    }

    public void testP() throws Exception {
        Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT, ByteRange> entry = strategy.defineRange(null, uri, null, null, null);
        Map<RdfCloudTripleStoreConstants.TABLE_LAYOUT, TripleRow> serialize = ryaTripleContext.serializeTriple(
                new RyaStatement(uri, uri, uri, null));
        TripleRow tripleRow = serialize.get(RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO);
        assertContains(entry.getValue(), tripleRow.getRow());
    }

    public void testHandles() throws Exception {
        //po(ng)
        assertTrue(strategy.handles(null, uri, uri, null));
        assertTrue(strategy.handles(null, uri, uri, uri));
        //po_r(s)(ng)
        assertTrue(strategy.handles(rangeURI, uri, uri, null));
        assertTrue(strategy.handles(rangeURI, uri, uri, uri));
        //p(ng)
        assertTrue(strategy.handles(null, uri, null, null));
        assertTrue(strategy.handles(null, uri, null, uri));
        //p_r(o)(ng)
        assertTrue(strategy.handles(null, uri, rangeURI, null));
        assertTrue(strategy.handles(null, uri, rangeURI, uri));
        //r(p)(ng)
        assertFalse(strategy.handles(null, rangeURI, null, null));
        assertFalse(strategy.handles(null, rangeURI, null, uri));

        //false cases
        //sp..
        assertFalse(strategy.handles(uri, uri, null, null));
        //r(s)_p
        assertFalse(strategy.handles(rangeURI, uri, null, null));
    }
}
