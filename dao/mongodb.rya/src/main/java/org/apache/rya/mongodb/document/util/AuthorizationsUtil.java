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
package org.apache.rya.mongodb.document.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.security.Authorizations;

import com.google.common.base.Charsets;

/**
 * Utility methods for {@link Authorizations}.
 */
public final class AuthorizationsUtil {
    /**
     * Private constructor to prevent instantiation.
     */
    private AuthorizationsUtil() {
    }

    /**
    * Gets the authorizations in sorted order.
    * @param authorizations the {@link Authorizations}.
    * @return authorizations, each as a string encoded in UTF-8
    */
    public static String[] getAuthorizationsStringArray(final Authorizations authorizations) {
        return getAuthorizationsStrings(authorizations).toArray(new String[0]);
    }

    /**
    * Gets the authorizations in sorted order. The returned list is not modifiable.
    * @param authorizations the {@link Authorizations}
    * @return authorizations, each as a string encoded in UTF-8
    */
    public static List<String> getAuthorizationsStrings(final Authorizations authorizations) {
        final List<String> copy = new ArrayList<>(authorizations.getAuthorizations().size());
        for (final byte[] auth : authorizations.getAuthorizations()) {
            copy.add(new String(auth, Charsets.UTF_8));
        }
        return Collections.unmodifiableList(copy);
    }
}