package mvm.rya.api.domain;

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

import org.junit.Assert;
import org.junit.Test;
import org.openrdf.model.vocabulary.XMLSchema;

public class RyaTypeTest {
    static RyaType a = new RyaType(XMLSchema.STRING, "http://www.example.com/Alice");
    static RyaType b = new RyaType(XMLSchema.STRING, "http://www.example.com/Bob");
    static RyaType c = new RyaType(XMLSchema.STRING, "http://www.example.com/Carol");
    static RyaType aUri = new RyaType(XMLSchema.ANYURI, "http://www.example.com/Alice");
    static RyaType bUri = new RyaType(XMLSchema.ANYURI, "http://www.example.com/Bob");
    RyaType nullData = new RyaType(XMLSchema.STRING, null);
    RyaType nullType = new RyaType(null, "http://www.example.com/Alice");
    RyaType nullBoth = new RyaType(null, null);
    RyaType same = new RyaType(XMLSchema.STRING, "http://www.example.com/Alice");

    @Test
    public void testCompareTo() throws Exception {
        Assert.assertEquals("compareTo(self) should return zero.", 0, aUri.compareTo(aUri));
        Assert.assertFalse("compareTo should return nonzero for different data and type.", aUri.compareTo(b) == 0);
        Assert.assertFalse("compareTo should return nonzero for same data and different datatypes.", a.compareTo(aUri) == 0);
        Assert.assertFalse("compareTo should return nonzero for same datatype and different data.", bUri.compareTo(aUri) == 0);
        Assert.assertEquals("compareTo should return zero for different objects with matching data and datatype.",
                0, a.compareTo(same));
    }

    @Test
    public void testCompareToNullFields() throws Exception {
        Assert.assertEquals("[has no nulls].compareTo([has null data]) should return -1", -1, a.compareTo(nullData));
        Assert.assertEquals("[has no nulls].compareTo([has null type]) should return -1 if data is equal",
                -1, a.compareTo(nullType));
        Assert.assertEquals("[has null data].compareTo([has no nulls]) should return 1", 1, nullData.compareTo(a));
        Assert.assertEquals("[has null type].compareTo([has no nulls]) should return 1 if data is equal",
                 1, nullType.compareTo(a));
        Assert.assertEquals("[has null type].compareTo([has null data]) should return -1", -1, nullType.compareTo(nullData));
    }

    @Test
    public void testCompareToSymmetry() throws Exception {
        int forward = Integer.signum(a.compareTo(b));
        int backward = Integer.signum(b.compareTo(a));
        Assert.assertEquals("Comparison of different values with same type should yield opposite signs.", forward, backward * -1);
        forward = Integer.signum(bUri.compareTo(b));
        backward = Integer.signum(b.compareTo(bUri));
        Assert.assertEquals("Comparison of same values with different types should yield opposite signs.", forward, backward*-1);
        forward = Integer.signum(aUri.compareTo(b));
        backward = Integer.signum(b.compareTo(aUri));
        Assert.assertEquals("Comparison of different values with different types should yield opposite signs.",
                forward, backward * -1);
    }

    @Test
    public void testCompareToTransitive() throws Exception {
        int sign = Integer.signum(a.compareTo(b));
        Assert.assertEquals("compareTo(a,b) and compareTo(b,c) should have the same sign.",
                sign, Integer.signum(b.compareTo(c)));
        Assert.assertEquals("if a > b > c, compareTo(a,c) should be consistent.", sign, Integer.signum(a.compareTo(c)));
    }

    @Test
    public void testEquals() throws Exception {
        Assert.assertTrue("Same data and datatype should be equal.", a.equals(same));
        Assert.assertFalse("Same data, different datatype should be unequal.", a.equals(aUri));
        Assert.assertFalse("Same datatype, different data should be unequal.", a.equals(b));
    }

    @Test
    public void testEqualsNullFields() throws Exception {
        Assert.assertFalse("equals(null) should return false.", a.equals(null));
        Assert.assertFalse("Same data, one null datatype should be unequal.", a.equals(nullType));
        Assert.assertFalse("Same datatype, one null data should be unequal.", a.equals(nullData));
        RyaType sameNull = new RyaType(null, null);
        Assert.assertTrue("Matching null fields should be equal.", sameNull.equals(nullBoth));
    }

    @Test
    public void testEqualsCompareToConsistency() throws Exception {
        Assert.assertEquals("equals and compareTo inconsistent for matching values and types.",
                a.equals(same), a.compareTo(same) == 0);
        Assert.assertEquals("equals and compareTo inconsistent for different values with same types.",
                a.equals(b), a.compareTo(b) == 0);
        Assert.assertEquals("equals and compareTo inconsistent for same values having different types.",
                a.equals(aUri), a.compareTo(aUri) == 0);
        Assert.assertEquals("equals and compareTo inconsistent for different values and different types.",
                a.equals(bUri), a.compareTo(bUri) == 0);
    }

    @Test
    public void testHashCodeEquals() throws Exception {
        Assert.assertEquals("Same data and same type should yield same hash code.",
                a.hashCode(), same.hashCode());
        Assert.assertEquals("Same type and both null data should yield same hash code.",
                nullData.hashCode(), new RyaType(XMLSchema.STRING, null).hashCode());
        Assert.assertEquals("Same data and both null type should yield same hash code.",
                nullType.hashCode(), new RyaType(null, "http://www.example.com/Alice").hashCode());
        Assert.assertEquals("Null type and null data should yield same hash code.",
                nullBoth.hashCode(), new RyaType(null, null).hashCode());
    }
}
