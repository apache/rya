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
package org.apache.rya.export.accumulo;

import java.util.Date;

import org.apache.rya.export.accumulo.util.AccumuloRyaUtils;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaURI;

/**
 * Utility methods for testing merging/copying.
 */
public final class TestUtils {
    private static final String NAMESPACE = "#:";

    /**
     * Creates a {@link RyaURI} for the specified local name.
     * @param localName the URI's local name.
     * @return the {@link RyraURI}.
     */
    public static RyaURI createRyaUri(final String localName) {
        return AccumuloRyaUtils.createRyaUri(NAMESPACE, localName);
    }

    /**
     * Converts a {@link RyaURI} to the contained data string.
     * @param namespace the namespace.
     * @param the {@link RyaURI} to convert.
     * @return the data value without the namespace.
     */
    public static String convertRyaUriToString(final RyaURI RyaUri) {
        return AccumuloRyaUtils.convertRyaUriToString(NAMESPACE, RyaUri);
    }

    /**
     * Creates a {@link RyaStatement} from the specified subject, predicate, and object.
     * @param subject the subject.
     * @param predicate the predicate.
     * @param object the object.
     * @param date the {@link Date} to use for the key's timestamp.
     * @return the {@link RyaStatement}.
     */
    public static RyaStatement createRyaStatement(final String subject, final String predicate, final String object, final Date date) {
        final RyaURI subjectUri = createRyaUri(subject);
        final RyaURI predicateUri = createRyaUri(predicate);
        final RyaURI objectUri = createRyaUri(object);
        final RyaStatement ryaStatement = new RyaStatement(subjectUri, predicateUri, objectUri);
        if (date != null) {
            ryaStatement.setTimestamp(date.getTime());
        }
        return ryaStatement;
    }

    /**
     * Copies a {@link RyaStatement} into a new {@link RyaStatement}.
     * @param s the {@link RyaStatement} to copy.
     * @return the newly copied {@link RyaStatement}.
     */
    public static RyaStatement copyRyaStatement(final RyaStatement s) {
        return new RyaStatement(s.getSubject(), s.getPredicate(), s.getObject(), s.getContext(), s.getQualifer(), s.getColumnVisibility(), s.getValue(), s.getTimestamp());
    }
}