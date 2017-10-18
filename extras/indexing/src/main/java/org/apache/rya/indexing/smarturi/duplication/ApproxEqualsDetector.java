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
package org.apache.rya.indexing.smarturi.duplication;

import org.apache.rya.indexing.smarturi.SmartUriException;
import org.eclipse.rdf4j.model.IRI;

/**
 * Interface for detecting if two objects of type {@code T} are considered
 * approximately equal to each other.
 * @param <T> the type of object the implementation of
 * {@link ApproxEqualsDetector} handles.
 */
public interface ApproxEqualsDetector<T> {
    /**
     * Checks if two objects are approximately equal.
     * @param lhs the left hand side object.
     * @param rhs the right hand side object.
     * @return {@code true} if the two objects are considered approximately
     * equals. {@code false} otherwise.
     */
    boolean areObjectsApproxEquals(final T lhs, final T rhs);

    /**
     * @return the default tolerance for the type.
     */
    Tolerance getDefaultTolerance();

    /**
     * Converts a string representation of the object into the object
     * represented by the class {@link #getTypeClass()}.
     * @param string the {@link String} to convert to an object.
     * @return the object.
     * @throws SmartUriException
     */
    T convertStringToObject(final String string) throws SmartUriException;

    /**
     * @return the object {@link Class} this detector is used for.
     */
    Class<?> getTypeClass();

    /**
     * @return the {@link IRI} for the XML schema type this detector is used
     * for.
     */
    IRI getXmlSchemaUri();

    /**
     * Checks if two string representations of objects are approximately equal.
     * @param lhs the left hand side string object representation.
     * @param rhs the right hand side string object representation.
     * @return {@code true} if the two string object representations are
     * considered approximately equals. {@code false} otherwise.
     * @throws SmartUriException
     */
    default boolean areApproxEquals(final String lhs, final String rhs) throws SmartUriException {
        final T object1 = convertStringToObject(lhs);
        final T object2 = convertStringToObject(rhs);
        return areObjectsApproxEquals(object1, object2);
    }
}