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
package org.apache.rya.api.query.strategy;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.TYPE_DELIM;

import java.nio.charset.StandardCharsets;

import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.resolver.triple.TripleRowRegex;

import com.google.common.base.Preconditions;

/**
 * Date: 7/14/12
 * Time: 8:06 AM
 */
public abstract class AbstractTriplePatternStrategy implements TriplePatternStrategy {
    public static final String ALL_REGEX = "([\\s\\S]*)";

    @Override
    public abstract RdfCloudTripleStoreConstants.TABLE_LAYOUT getLayout();

    @Override
    public TripleRowRegex buildRegex(final String subject, final String predicate, final String object, final String context, final byte[] objectTypeInfo) {
        final RdfCloudTripleStoreConstants.TABLE_LAYOUT table_layout = getLayout();
        Preconditions.checkNotNull(table_layout);
        if (subject == null && predicate == null && object == null && context == null && objectTypeInfo == null) {
            return null; //no regex
        }
        final StringBuilder sb = new StringBuilder();
        String first = subject;
        String second = predicate;
        String third = object;
        if (table_layout == RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO) {
            first = predicate;
            second = object;
            third = subject;
        } else if (table_layout == RdfCloudTripleStoreConstants.TABLE_LAYOUT.OSP) {
            first = object;
            second = subject;
            third = predicate;
        }

        if (first != null) {
            sb.append(first);
        } else {
            sb.append(ALL_REGEX);
        }
        sb.append(DELIM);

        if (second != null) {
            sb.append(second);
        } else {
            sb.append(ALL_REGEX);
        }
        sb.append(DELIM);

        if (third != null) {
            sb.append(third);
            if (objectTypeInfo == null) {
                sb.append(TYPE_DELIM);
                sb.append(ALL_REGEX);
            }else {
                sb.append(new String(objectTypeInfo, StandardCharsets.UTF_8));
            }
        }else {
            sb.append(ALL_REGEX);
            if (objectTypeInfo != null) {
                sb.append(new String(objectTypeInfo, StandardCharsets.UTF_8));
            }
        }

        return new TripleRowRegex(sb.toString(), (context != null) ? (context + ALL_REGEX) : null, null);
    }
}
