package org.apache.rya.indexing.accumulo.temporal;

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


import java.util.Arrays;

import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.TemporalInterval;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

public class TemporalIntervalTest {
    @Test
    public void constructorTest() throws Exception {
        TemporalInterval ti = new TemporalInterval( //
                new TemporalInstantRfc3339(2014, 12, 30, 12, 59, 59), //
                new TemporalInstantRfc3339(2014, 12, 30, 13, 00, 00)); //
        Assert.assertNotNull(ti.getAsKeyBeginning());
        Assert.assertNotNull(ti.getHasEnd());
    }
    @Test
    public void constructorBadArgTest() throws Exception {
        // the end precedes the beginning:
        try {
            TemporalInterval ti = new TemporalInterval( //
                new TemporalInstantRfc3339(2017, 12, 30, 12, 59, 59), //
                new TemporalInstantRfc3339( 820, 12, 30, 12, 59, 59)); // the invention of algebra.
            Assert.assertFalse("Constructor should throw an error if the beginning is after the end, but no error for interval:"+ti, true);
        }catch (IllegalArgumentException e) {
            // expected to catch this error.
        }
    }

    @Test
    public void relationsTest() throws Exception {

        TemporalInterval ti01 = new TemporalInterval(
                new TemporalInstantRfc3339(2015, 12, 30, 12, 59, 59), //
                new TemporalInstantRfc3339(2016, 12, 30, 13, 00, 00)); //

        TemporalInterval ti02 = new TemporalInterval(
                new TemporalInstantRfc3339(2015, 12, 30, 12, 59, 59), //
                new TemporalInstantRfc3339(2016, 12, 30, 13, 00, 00)); //

        Assert.assertTrue("same constructor parameters, should be equal.",
                ti01.equals(ti02));
        Assert.assertTrue(
                "same constructor parameters, should compare 0 equal.",
                0 == ti01.compareTo(ti02));

    }

    @Test
    public void keyBeginTest() throws Exception {
        // 58 seconds, earlier
        TemporalInterval beginTI01 = new TemporalInterval(
                new TemporalInstantRfc3339(2015, 12, 30, 12, 59, 58), //
                new TemporalInstantRfc3339(2016, 12, 30, 13, 00, 00)); //
        // 59 seconds, later
        TemporalInterval beginTI02 = new TemporalInterval(
                new TemporalInstantRfc3339(2015, 12, 30, 12, 59, 59), //
                new TemporalInstantRfc3339(2016, 12, 30, 13, 00, 00)); //

        String key01b = Arrays.toString(beginTI01.getAsKeyBeginning());
        String key02b = Arrays.toString(beginTI02.getAsKeyBeginning());
        Assert.assertEquals("key02 is later so comparesTo = 1.", 1, key02b.compareTo(key01b));
        Assert.assertEquals("key01 is first so comparesTo = -1", -1, key01b.compareTo(key02b));

    }
    // These are shared for end test and compareTo tests.
    // tiB03_E20 read as: Begins 3 seconds, ends at 20 seconds
    final TemporalInterval tiB03_E20 = new TemporalInterval(//
            new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 03), //
            new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 20));
    // 30 seconds, Begins earlier, ends later
    final TemporalInterval tiB02_E30 = new TemporalInterval(//
            new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 02), //
            new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 30));
    // 30 seconds, same as above
    final TemporalInterval tiB02_E30Dup = new TemporalInterval(//
            new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 02), //
            new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 30));
    // 30 seconds, same as above, but ends later
    final TemporalInterval tiB02_E31 = new TemporalInterval(//
            new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 02), //
            new TemporalInstantRfc3339(2015, 12, 30, 12, 00, 31));

    @Test
    public void CompareToTest() throws Exception {

        Assert.assertEquals("interval tiB03_E20.compareTo(tiB02_E30), B03 starts later(greater) so comparesTo =  1.", 1, tiB03_E20.compareTo(tiB02_E30));
        Assert.assertEquals("interval tiB02_E30.compareTo(tiB03_E20), B02 starts first(lesser)  so comparesTo = -1", -1, tiB02_E30.compareTo(tiB03_E20));
        Assert.assertEquals("interval tiB02_E30.compareTo(tiB02_E31), E30 ends   first so comparesTo = -1", -1, tiB02_E30.compareTo(tiB02_E31));
        Assert.assertEquals("interval tiB02_E30.compareTo(tiB02_E30Dup) same so comparesTo =  0", 0, tiB02_E30.compareTo(tiB02_E30Dup));
    }
    @Test
    public void EqualsTest() throws Exception {
        Object notATemporalInterval = "Iamastring.";
        Assert.assertFalse("interval tiB02_E30.equals(tiB02_E31) differ so equals() is false.",  tiB02_E30.equals(notATemporalInterval));
        Assert.assertFalse("interval tiB02_E30.equals(tiB02_E31) differ so equals() is false.",  tiB02_E30.equals(tiB02_E31));
        Assert.assertTrue ("interval tiB02_E30.equals(tiB02_E30Dup) same so equals() is true.",  tiB02_E30.equals(tiB02_E30Dup));
    }
    @Test
    public void keyEndTest() throws Exception {
        String keyB03_E20 = new String( tiB03_E20.getAsKeyEnd(), "US-ASCII");
        String keyB02_E30 = new String(tiB02_E30.getAsKeyEnd(), "US-ASCII");
        String keyB02_E30Dup = new String(tiB02_E30Dup.getAsKeyEnd(), "US-ASCII");
        
        Assert.assertEquals("End keyB02_E30.compareTo(keyB03_E20), E30 is later =  1. key="+keyB02_E30, 1, keyB02_E30.compareTo(keyB03_E20));
        Assert.assertEquals("End keyB03_E20.compareTo(keyB02_E30), E20 is first = -1", -1, keyB03_E20.compareTo(keyB02_E30));
        Assert.assertEquals("End keyB02_E30.compareTo(keyB02_E30Dup) same so comparesTo =  0", 0, keyB02_E30.compareTo(keyB02_E30Dup));
    }

    

    @Test
     public void infinitePastFutureAlwaysTest() throws Exception {
         final TemporalInstant TestDateString = new TemporalInstantRfc3339(new DateTime("2015-01-01T01:59:59Z"));
         TemporalInterval tvFuture = new TemporalInterval(TestDateString,TemporalInstantRfc3339.getMaximumInstance());
         TemporalInterval tvPast = new TemporalInterval(TemporalInstantRfc3339.getMinimumInstance(), TestDateString);
         TemporalInterval tvAlways = new TemporalInterval(TemporalInstantRfc3339.getMinimumInstance(), TemporalInstantRfc3339.getMaximumInstance());
         Assert.assertTrue("The future is greater (starts after) than the past for compareTo().",       tvFuture.compareTo(tvPast) > 0);
         Assert.assertTrue("The future is greater (starts after) than always for compareTo().",         tvFuture.compareTo(tvAlways) > 0);
         Assert.assertTrue("The past is less (starts same, ends earlier) than always for compareTo().", tvFuture.compareTo(tvPast) > 0);
         
    }
    @Test
    public void hashTest() throws Exception {
        // Use MAX to see how it handles overflowing values.  Should silently go negative.
        int hashcode01Same = (new TemporalInterval(new TemporalInstantRfc3339(new DateTime(Integer.MAX_VALUE / 2)), new TemporalInstantRfc3339(new DateTime(Integer.MAX_VALUE)))).hashCode();
        int hashcode02Same = (new TemporalInterval(new TemporalInstantRfc3339(new DateTime(Integer.MAX_VALUE / 2)), new TemporalInstantRfc3339(new DateTime(Integer.MAX_VALUE)))).hashCode();
        int hashcode03Diff = (new TemporalInterval(new TemporalInstantRfc3339(new DateTime(Integer.MAX_VALUE / 2)), new TemporalInstantRfc3339(new DateTime(Integer.MAX_VALUE)))).hashCode();
        int hashcode04Diff = (new TemporalInterval(new TemporalInstantRfc3339(new DateTime(Integer.MIN_VALUE    )), new TemporalInstantRfc3339(new DateTime(Integer.MIN_VALUE)))).hashCode();
        int hashcode05Diff = (new TemporalInterval(new TemporalInstantRfc3339(new DateTime(Integer.MAX_VALUE    )), new TemporalInstantRfc3339(new DateTime(Integer.MAX_VALUE)))).hashCode();
        int hashcode06Diff = (new TemporalInterval(new TemporalInstantRfc3339(new DateTime(0)), new TemporalInstantRfc3339(new DateTime( 0)))).hashCode();
        int hashcode07Diff = (new TemporalInterval(new TemporalInstantRfc3339(new DateTime(1000)), new TemporalInstantRfc3339(new DateTime( 1000)))).hashCode();
        int hashcode08Diff = (new TemporalInterval(new TemporalInstantRfc3339(new DateTime(0)), new TemporalInstantRfc3339(new DateTime( 1000)))).hashCode();
        int hashcode09Diff = (new TemporalInterval((TemporalInstantRfc3339.getMinimumInstance()),(TemporalInstantRfc3339.getMaximumInstance()) )).hashCode();
        int hashcode10Diff = (new TemporalInterval(new TemporalInstantRfc3339(new DateTime(0))  ,(TemporalInstantRfc3339.getMaximumInstance()) )).hashCode();
        Assert.assertEquals("Same input should produce same hashcode. (always!)", hashcode01Same , hashcode02Same);

        Assert.assertTrue("Different small input should produce different hashcode. (usually!) hashcodes:"
                +hashcode03Diff+" "+hashcode04Diff+" "+hashcode03Diff+" "+hashcode05Diff,
                hashcode03Diff != hashcode04Diff && hashcode03Diff != hashcode05Diff);

        Assert.assertTrue("Different large input should produce different hashcode. (usually!) hashcodes:"
                +hashcode06Diff +" "+ hashcode07Diff +" "+ hashcode06Diff +" "+ hashcode08Diff
                +" key for date 0= "+(new TemporalInstantRfc3339(new DateTime(0))).getAsKeyString()
                +" key for date 1000= "+(new TemporalInstantRfc3339(new DateTime(1000))).getAsKeyString(),
                hashcode06Diff != hashcode07Diff && hashcode06Diff != hashcode08Diff);
        Assert.assertTrue("Different max and min input should produce different hashcode. (usually!) hashcodes:"
                +hashcode09Diff +" != "+ hashcode10Diff 
                +"fyi: key for date max= "+(TemporalInstantRfc3339.getMaximumInstance()).getAsKeyString()
                +    " key for date min= "+(TemporalInstantRfc3339.getMinimumInstance()).getAsKeyString(),
                hashcode09Diff != hashcode10Diff );

    }
}
