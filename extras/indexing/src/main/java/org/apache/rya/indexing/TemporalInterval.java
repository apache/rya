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


import java.io.UnsupportedEncodingException;

/**
 * A time with beginning and end date and time, which could be indefinitely in
 * the past or future. Immutable, so it's thread safe. For use in reading and
 * writing from Rya's temporal indexing scheme.
 * 
 */
public class TemporalInterval implements Comparable<TemporalInterval> {

	 // the beginning and end.  Read-only because they are final references to immutable objects.
    private final TemporalInstant hasBeginning;
    private final TemporalInstant hasEnd;

    /**
     * Separate the beginning and end with this.
     * Used because Joda time library's interval uses this.
     * TODO: Move this down to the TemporalInterval implementation. 
     * TODO: Then add a TemporalInterval.keyConcatenate().
     */
    public static final String DELIMITER = "/";

//    /**
//     * Empty constructor -- not allowed, no defaults. 
//     * For an infinite span of time: do it like this:     
//     *     new TemporalInterval(TemporalInstantImpl.getMinimum, TemporalInstantImpl.getMaximum)
//     */
//    public TemporalInterval() {
//        hasBeginning = null;
//        hasEnd = null;
//    }

    /**
     * Constructor setting beginning and end with an implementation of {@link TemporalInstant}.
     * beginning must be less than end.
     * 
     * @param hasBeginning
     * @param hasEnd
     */
    public TemporalInterval(TemporalInstant hasBeginning, TemporalInstant hasEnd) {
        super();
        if (hasBeginning != null && hasEnd != null && 0 < hasBeginning.compareTo(hasEnd))
            throw new IllegalArgumentException("The Beginning instance must not compare greater than the end.");
        this.hasBeginning = hasBeginning;
        this.hasEnd = hasEnd;
    }

    /**
	 * @return the hasBeginning
	 */
	public TemporalInstant getHasBeginning() {
		return hasBeginning;
	}

	/**
	 * @return the hasEnd
	 */
	public TemporalInstant getHasEnd() {
		return hasEnd;
	}

	/**
     * True if CompareTo() says equal (0)
     */
    @Override
    public boolean equals(Object other) {
        return other instanceof TemporalInterval
                && this.compareTo((TemporalInterval) other) == 0;
    };

    /**
     * Compare beginnings, if the same then compare ends, or equal if beginnings equal and endings equal.
     * Nulls represent infinity.
     */
    @Override
    public int compareTo(TemporalInterval other) {
        int compBegins = this.hasBeginning.compareTo(other.hasBeginning);
        if (0 == compBegins)
            return this.hasEnd.compareTo(other.hasEnd);
        else
            return compBegins;

    }

    /**
     * Hashcode for
     */
    @Override
    public int hashCode() {
        if (hasBeginning == null)
            if (hasEnd == null)
                return 0;
            else
                return hasEnd.hashCode();
        else
            return hashboth(this.hasBeginning.hashCode(),
                    this.hasEnd.hashCode());
    }

    /**
     * Hashcode combining two string hashcodes.
     */
    protected static int hashboth(int i1, int i2) {
        // return (int) (( 1L * i1 * i2) ; % (1L + Integer.MAX_VALUE));
        // let the overflow happen. It won't throw an error.
        return (i1 + i2);
    }

    /**
     * Get the key use for rowid for the beginning of the interval. Use ascii
     * for conversion to catch and prevent multi-byte chars.
     * 
     * @return
     */
    public byte[] getAsKeyBeginning() {
        try {
            return (hasBeginning.getAsKeyString() + DELIMITER + hasEnd
                    .getAsKeyString()).getBytes("US-ASCII");
        } catch (UnsupportedEncodingException e) {
            // this is a code error, the strings are mostly numbers.
            throw new Error("while converting key string to ascii bytes", e);
        }
    }

    /**
     * get the key used for indexing the end of the interval. Use ascii for
     * conversion to catch and prevent multi-byte chars.
     * 
     * @return
     */
    public byte[] getAsKeyEnd() {
        try {
            return (hasEnd.getAsKeyString() + DELIMITER + hasBeginning
                    .getAsKeyString()).getBytes("US-ASCII");
        } catch (UnsupportedEncodingException e) {
            // this is a code error, the strings are mostly numbers and ascii
            // symbols.
            throw new Error("while converting key string to ascii bytes", e);
        }
    }

    /**
     * Format as a "period" in this paper.  This is not a standard, really.
     * http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.298.8948&rep=rep1&type=pdf
     * also consider using the typed literal syntax:
     * "[2010-01-01,2010-01-31]"^^xs:period
     * @return [begindate,enddate] for example: [2010-01-01,2010-01-31]
     * 
     */
    public String getAsPair() {
        return "["+hasBeginning.getAsReadable() + "," + hasEnd.getAsReadable() + "]";
    }

    @Override
    public String toString() {
    	return getAsPair() ;
    	// return hasBeginning.getAsReadable() + DELIMITER + hasEnd.getAsReadable();
    }
}
