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



import static org.junit.Assert.*;

import java.util.Date;
import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaTypeResolverException;

import org.junit.Ignore;
import org.junit.Test;
import org.openrdf.model.impl.CalendarLiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;

/**
 * Test serializing and deserializing.
 * Notes:
 * The serialization, deserialization fills in some information:
 * If preserving uncertainty, or preserving the source timezone, then don't use XML type tag.
 * 		- uncertainty: missing time hh:mm:ss becomes 00:00:00 
 * 		- uncertainty: missing milliseconds (.123) become .000.
 * 		- uncertainty: missing timezone becomes the system local timezone.
 * 		- timezone: converted to the equivalent Z timezone.  
 * 		- a type XMLSchema.DATE become XMLSchema.DATETIME after deserialized
 * 
 * 		ex: run in timezone eastern time (GMT-5:00): 
 * 			before=       2000-02-02                 type = XMLSchema.DATE
 * 			deserialized= 2000-02-02T05:00:00.000Z   type = XMLSchema.DATETIME
 */
public class DateTimeRyaTypeResolverTest {
	@Test
    public void testDateTime() throws Exception {
        long currentTime = 1342182689285l;
        Date date = new Date(currentTime);
        GregorianCalendar gc = new GregorianCalendar();
        gc.setTimeInMillis(date.getTime());
        XMLGregorianCalendar xmlGregorianCalendar = DatatypeFactory.newInstance().newXMLGregorianCalendar(gc);
        CalendarLiteralImpl literal = new CalendarLiteralImpl(xmlGregorianCalendar);
        byte[] serialize = new DateTimeRyaTypeResolver().serialize(RdfToRyaConversions.convertLiteral(literal));
        RyaType deserialize = new DateTimeRyaTypeResolver().deserialize(serialize);
        assertEquals("2012-07-13T12:31:29.285Z", deserialize.getData());
        assertEquals(XMLSchema.DATETIME, deserialize.getDataType());
    }
	@Test
    public void testFull() throws Exception {
        String currentTime = "2000-01-01T00:00:01.111Z";
		assertSerializeAndDesDateTime("2000-01-01T00:00:01.111Z");
		
    }
	@Test
    public void testNoMilliSeconds() throws Exception {
		assertSerializeAndDesDateTime("2000-01-01T00:00:01Z","2000-01-01T00:00:01.000Z");
		
    }
	@Test
    public void testDateNoTimeNoZone() throws Exception {
        String beforeDate = "2000-02-02";
    	String afterDate="2000-02-0(1|2|3)T\\d\\d:\\d\\d:00\\.000Z";
    	RyaType deserialize = serializeAndDeserialize(beforeDate, XMLSchema.DATE);
	    final String afterActual = deserialize.getData();
		assertTrue("Before='"+beforeDate+"'; Expected should match actual regex after='"+afterDate+"' deserialized:"+afterActual, afterActual.matches(afterDate));
        assertEquals(XMLSchema.DATETIME, deserialize.getDataType());
    }
	@Test
    public void testDateZoneNoTime() throws Exception {
		// if you see this:
		//java.lang.IllegalArgumentException: Invalid format: "2000-02-02Z" is malformed at "Z"
		// use this: "2000-02-02TZ";
        String currentTime = "2000-02-02TZ";
    	RyaType deserialize = serializeAndDeserialize(currentTime, XMLSchema.DATE);
        assertEquals("Before expected should match after actual deserialized:","2000-02-02T00:00:00.000Z", deserialize.getData());
        assertEquals(XMLSchema.DATETIME, deserialize.getDataType());
    }
	@Test
    public void testNoZone() throws Exception {
		String beforeDate = "2000-01-02T00:00:01";
    	String afterDate="2000-01-0(1|2|3)T\\d\\d:\\d\\d:01\\.000Z";
    	RyaType deserialize = serializeAndDeserialize(beforeDate, XMLSchema.DATE);
	    final String afterActual = deserialize.getData();
		assertTrue("Before='"+beforeDate+"'; Expected should match actual regex after='"+afterDate+"' deserialized:"+afterActual, afterActual.matches(afterDate));
        assertEquals(XMLSchema.DATETIME, deserialize.getDataType());
		
    }
    @Test
	public void testMilliSecondsNoZone() throws Exception {
    	String beforeDate="2002-02-02T02:02:02.222";
	String afterDate="2002-02-0(1|2|3)T\\d\\d:\\d\\d:02\\.222.*";
		RyaType deserialize = serializeAndDeserialize(beforeDate, XMLSchema.DATETIME);
	    final String afterActual = deserialize.getData();
		assertTrue("Before='"+beforeDate+"'; Expected should match actual regex after='"+afterDate+"' deserialized:"+afterActual, afterActual.matches(afterDate));
	    assertEquals(XMLSchema.DATETIME, deserialize.getDataType());
		
	}
    @Test
	public void testHistoryAndFuture() throws Exception {
		assertSerializeAndDesDateTime("-2000-01-01T00:00:01Z","-2000-01-01T00:00:01.000Z");
		assertSerializeAndDesDateTime("111-01-01T00:00:01Z","0111-01-01T00:00:01.000Z");
		assertSerializeAndDesDateTime("12345-01-01T00:00:01Z","12345-01-01T00:00:01.000Z");
	}

    @Test
	public void testTimeZone() throws Exception {
		assertSerializeAndDesDateTime(    "2000-01-01T00:00:01+01:00", "1999-12-31T23:00:01.000Z");
		assertSerializeAndDesDateTime(    "2000-01-01T00:00:01+02:30", "1999-12-31T21:30:01.000Z");
		assertSerializeAndDesDateTime("2000-01-01T00:00:01.123-02:00", "2000-01-01T02:00:01.123Z");
		assertSerializeAndDesDateTime(     "111-01-01T00:00:01+14:00", "0110-12-31T10:00:01.000Z" );
		assertSerializeAndDesDateTime(   "12345-01-01T00:00:01-14:00","12345-01-01T14:00:01.000Z");
		assertSerializeAndDesDateTime(       "1-01-01T00:00:01+14:00", "0000-12-31T10:00:01.000Z" ); 
	}

    @Test
    public void testGarbageIn() throws Exception {
        String currentTime = "Blablabla";
		RyaType ryaType = new RyaType(XMLSchema.DATETIME, currentTime );
		Throwable threw=null;
		try {
			new DateTimeRyaTypeResolver().serialize(ryaType);
		} catch (java.lang.IllegalArgumentException exception) {
			threw = exception;
		}
		assertNotNull("Expected to catch bad format message.",threw);
		assertEquals("Caught bad format message.","Invalid format: \"Blablabla\"", threw.getMessage());
    }
	/**
	 * Do the test on the DateTime
	 * @param dateTimeString
	 * @throws RyaTypeResolverException
	 */
	private void assertSerializeAndDesDateTime(String dateTimeString) throws RyaTypeResolverException {
		assertSerializeAndDesDateTime(dateTimeString, dateTimeString); 
	}
	private void assertSerializeAndDesDateTime(String beforeDate, String afterDate ) throws RyaTypeResolverException {
		RyaType deserialize = serializeAndDeserialize(beforeDate, XMLSchema.DATETIME);
	    assertEquals("Before='"+beforeDate+"'; Expected should match actual after deserialized:",afterDate, deserialize.getData());
	    assertEquals(XMLSchema.DATETIME, deserialize.getDataType());
	}
	/**
	 * Serialize a datetime string, then deserialize as a ryaType.
	 * @param dateTimeString
	 * @param type if null , use default: XMLSchema.DATETIME
	 * @return
	 * @throws RyaTypeResolverException
	 */
	private RyaType serializeAndDeserialize(String dateTimeString, org.openrdf.model.URI type ) throws RyaTypeResolverException {
		if (type == null) 
			type = XMLSchema.DATETIME;
		RyaType ryaType = new RyaType(type, dateTimeString ); 
	    byte[] serialize = new DateTimeRyaTypeResolver().serialize(ryaType);
	    return new DateTimeRyaTypeResolver().deserialize(serialize);
	}
}
