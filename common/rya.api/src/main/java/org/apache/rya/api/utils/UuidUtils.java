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
package org.apache.rya.api.utils;

import static java.util.Objects.requireNonNull;

import java.util.UUID;

/**
 * Utility methods and constants for {@link UUID}s.
 */
public final class UuidUtils {
    /**
     * The length of a {@link UUID} string. It is 32 characters long and has 4
     * hyphens for a total of 36 characters.
     */
    public static final int UUID_STRING_LENGTH = 36;

    /**
     * Private constructor to prevent instantiation.
     */
    private UuidUtils() {
    }

    /**
     * Extracts a UUID from the end of a string.
     * @param text the string to extract from. (not {@code null}.
     * @return the {@link UUID}.
     */
    public static UUID extractUuidFromStringEnd(final String text) {
        requireNonNull(text);
        final String uuidString = text.substring(text.length() - UUID_STRING_LENGTH, text.length());
        final UUID uuid = UUID.fromString(uuidString);
        return uuid;
    }
}