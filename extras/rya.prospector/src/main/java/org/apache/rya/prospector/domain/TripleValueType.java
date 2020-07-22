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
package org.apache.rya.prospector.domain;

import com.google.common.collect.ImmutableMap;

import static java.util.Objects.requireNonNull;

/**
 * Enumerates the different types of counts that are performed over a Rya instance's
 * Statements as part of a Prospector run.
 */
public enum TripleValueType {
    /**
     * The data portion of an {@link IndexEntry} contains a unique Subject that
     * appears within a Rya instance's Statements.
     */
    SUBJECT("subject"),

    /**
     * The data portion of an {@link IndexEntry} contains a unique Predicate that
     * appears within a Rya instance's Statements.
     */
    PREDICATE("predicate"),

    /**
     * The data portion of an {@link IndexEntry} contains a unique Object that
     * appears within a Rya instance's Statements.
     */
    OBJECT("object"),

    /**
     * The data portion of an {@link IndexEntry} contains a unique Namespace from
     * the Subjects that appear within a Rya instance.
     */
    ENTITY("entity"),

    /**
     * The data portion of an {@link IndexEntry} contains a unique Subject and Predicate
     * pair that appears within a Rya instance's Statements.
     */
    SUBJECT_PREDICATE("subjectpredicate"),

    /**
     * The data portion of an {@link IndexEntry} contains a unique Predicate and Object
     * pair that appears within a Rya instance's Statements.
     */
    PREDICATE_OBJECT("predicateobject"),

    /**
     * The data portion of an {@link IndexEntry} contains a unique Subject and Object
     * pair that appears within a Rya instance's Statements.
     */
    SUBJECT_OBJECT("subjectobject");
	
	private final String indexType;
	
	private TripleValueType(String indexType) {
		this.indexType = requireNonNull(indexType);
	}
	
	/**
	 * @return The Prospector Index Type represented by the enum value. 
	 */
	public String getIndexType() {
		return indexType;
	}
	
	private static final ImmutableMap<String, TripleValueType> lookup;
	static {
		ImmutableMap.Builder<String, TripleValueType> builder = ImmutableMap.builder();
		for(TripleValueType type : TripleValueType.values()) {
			builder.put(type.getIndexType(), type);
		}
		lookup = builder.build();
	}
	
	/**
	 * @param indexType - The index name to lookup.
	 * @return The enum value that represents the index name.
	 */
	public TripleValueType fromIndexType(String indexType) {
		return lookup.get(indexType);
	}
}