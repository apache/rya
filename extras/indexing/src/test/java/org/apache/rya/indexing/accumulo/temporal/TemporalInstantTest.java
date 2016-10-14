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


import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.rya.indexing.TemporalInstant;
import org.apache.rya.indexing.TemporalInstantRfc3339;

import org.apache.commons.codec.binary.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Assert;
import org.junit.Test;

public class TemporalInstantTest {
	@Test
	public void constructorTest() throws Exception {
		TemporalInstant instant = new TemporalInstantRfc3339(2014, 12, 30, //
				12, 59, 59);
		// YYYY-MM-DDThh:mm:ssZ
		String stringTestDate01 = "2014-12-30T12:59:59Z";
		Date dateTestDate01 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
				.parse(stringTestDate01);
		Assert.assertEquals(stringTestDate01, instant.getAsKeyString());
		Assert.assertArrayEquals(StringUtils.getBytesUtf8(instant.getAsKeyString()), instant.getAsKeyBytes());
		Assert.assertTrue("Key must be normalized to time zone Zulu",instant.getAsKeyString().endsWith("Z"));
		// show the local time us.
		// Warning, if test is run in the London, or Zulu time zone, this test will be same as above, with the Z.
		// TimeZone.setDefault(TimeZone.getTimeZone("UTC")); // this does not affect the library, don't use.
		String stringLocalTestDate01 = new SimpleDateFormat( 	 "yyyy-MM-dd'T'HH:mm:ssXXX").format(dateTestDate01);
		// for ET, will be: "2014-12-30T07:59:59-05:00"
		//instant.getAsDateTime().withZone(null);
//		System.out.println("===System.getProperty(user.timezone)="+System.getProperty("user.timezone")); //=ET
//		System.out.println("===============TimeZone.getDefault()="+TimeZone.getDefault());				//=ET
//		System.out.println("===========DateTimeZone.getDefault()="+DateTimeZone.getDefault());			//=UTC (wrong!)
		// the timezone default gets set to UTC by some prior test, fix it here.
		DateTimeZone newTimeZone = null;
		try {
            String id = System.getProperty("user.timezone");
            if (id != null) {
                newTimeZone = DateTimeZone.forID(id);
            }
        } catch (RuntimeException ex) {
            // ignored
        }
        if (newTimeZone == null) {
            newTimeZone = DateTimeZone.forTimeZone(TimeZone.getDefault());
        }
        DateTimeZone.setDefault(newTimeZone);
        // null timezone means use the default:
		Assert.assertEquals("Joda time library (actual) should use same local timezone as Java date (expected).",	stringLocalTestDate01, instant.getAsReadable(null));
	}
	@Test
	public void zoneTestTest() throws Exception {
        final String ZONETestDateInBRST = "2014-12-31T23:59:59-02:00"; // arbitrary zone, BRST=Brazil, better if not local.
		final String ZONETestDateInZulu = "2015-01-01T01:59:59Z";
		final String ZONETestDateInET   = "2014-12-31T20:59:59-05:00";
		TemporalInstant instant = new TemporalInstantRfc3339(DateTime.parse(ZONETestDateInBRST));
		
		Assert.assertEquals("Test our test Zulu, ET  strings.", ZONETestDateInET,   DateTime.parse(ZONETestDateInZulu).withZone(DateTimeZone.forID("-05:00")).toString(ISODateTimeFormat.dateTimeNoMillis()));
        Assert.assertEquals("Test our test BRST,Zulu strings.", ZONETestDateInZulu, DateTime.parse(ZONETestDateInBRST).withZone(DateTimeZone.UTC).toString(ISODateTimeFormat.dateTimeNoMillis()));
        
		Assert.assertTrue("Key must be normalized to time zone Zulu: "+instant.getAsKeyString(),  instant.getAsKeyString().endsWith("Z"));
		Assert.assertEquals("Key must be normalized from BRST -02:00", ZONETestDateInZulu, instant.getAsKeyString());
		Assert.assertArrayEquals(StringUtils.getBytesUtf8(instant.getAsKeyString()),  instant.getAsKeyBytes());

		Assert.assertTrue(   "Ignore original time zone.", ! ZONETestDateInBRST.equals( instant.getAsReadable(DateTimeZone.forID("-07:00"))));
		Assert.assertEquals( "Use original time zone.",      ZONETestDateInBRST, instant.getAsDateTime().toString(TemporalInstantRfc3339.FORMATTER));
        Assert.assertEquals( "Time at specified time zone.", ZONETestDateInET,   instant.getAsReadable(DateTimeZone.forID("-05:00")));
		
        instant = new TemporalInstantRfc3339(DateTime.parse(ZONETestDateInZulu));
		Assert.assertEquals("expect a time with specified time zone.",  ZONETestDateInET, instant.getAsReadable(DateTimeZone.forID("-05:00")));
	}

}
