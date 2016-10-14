package org.apache.rya.indexing;

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


import java.io.Serializable;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Time and date interface for building intervals. 
 *
 *Implementations:
 *	Implementation should have a factory method for TemporalInterval since TemporalIntervals reference only this 
 *  interface for begin & end,  so it injects an implementation.
 *      public static TemporalInterval parseInterval(String dateTimeInterval)
 *      
 *  The following are notes and may not have been implemented.
 *  
 *  = rfc3339
 *https://www.ietf.org/rfc/rfc3339.txt
 * a subset of ISO-8601
 * YYYY-MM-DDThh:mm:ss.fffZ
 * Limits:
 *All dates and times are assumed to be in the "current era",
      somewhere between 0000AD and 9999AD.
 * resolution: to the second, or millisecond if the optional fraction is used.
 * 
 * = epoch
 * 32bit or 64bit integer specifying the number of seconds since a standard date-time (1970)
 * 32bit is good until 2038.
 * 64bit is good until after the heat death of our universe
 * 
 */
public interface TemporalInstant extends Comparable<TemporalInstant>, Serializable  {
    @Override
	public boolean equals(Object obj) ;
	
	@Override
	public int compareTo(TemporalInstant o) ;

	@Override
	public int hashCode() ;
	/**
	 * Get the date as a byte array.
	 */
	public byte[] getAsKeyBytes();
	/**
	 * Get the date as a String.
	 */
	public String getAsKeyString();
	/**
	 * Get the date as a human readable for reporting with timeZone.
	 */
	public String getAsReadable(DateTimeZone tz);
    /**
     * Get the date as a human readable for reporting, timeZone is implementation specific.
     */
    public String getAsReadable();
	/**
	 * Get the date as a Joda/Java v8 DateTime.
	 */
	public DateTime getAsDateTime();

}
