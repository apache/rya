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

package org.apache.rya.api.function.temporal;

/**
 * Constants for the Temporal Functions used in rya.
 */
public class TemporalIRIs {
    /**
     * All temporal functions have the namespace (<tt>http://rya.apache.org/ns/temporal#</tt>).
     */
    public static final String NAMESPACE = "http://rya.apache.org/ns/temporal#";

    /** <tt>http://rya.apache.org/ns/temporal#equals</tt> */
    public final static String EQUALS = NAMESPACE + "equals";

    /** <tt>http://rya.apache.org/ns/temporal#before</tt> */
    public final static String BEFORE = NAMESPACE + "before";

    /** <tt>http://rya.apache.org/ns/temporal#after</tt> */
    public final static String AFTER = NAMESPACE + "after";

    /** <tt>http://rya.apache.org/ns/temporal#within</tt> */
    public final static String WITHIN = NAMESPACE + "within";
}
