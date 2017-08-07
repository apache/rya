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
package org.apache.rya.indexing.entity.storage.mongo.key;

import org.javatuples.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Utilities to escape keys to be safe for MongoDB use.
 */
public final class MongoDbSafeKey {
    /**
     * Encode characters in this order.
     */
    private static final ImmutableList<Pair<String, String>> ESCAPE_CHARACTERS = ImmutableList.of(
            new Pair<>("\\", "\\\\"),
            new Pair<>("\\$", "\\u0024"),
            new Pair<>(".", "\\u002e")
        );

    /**
     * Decode characters in this order (reverse order of encoding).
     */
    private static final ImmutableList<Pair<String, String>> UNESCAPE_CHARACTERS = ImmutableList.copyOf(Lists.reverse(ESCAPE_CHARACTERS));

    /**
     * Private constructor to prevent instantiation.
     */
    private MongoDbSafeKey() {
    }

    /**
     * Encodes a string to be safe for use as a MongoDB field name.
     * @param key the unencoded key.
     * @return the encoded key.
     */
    public static String encodeKey(final String key) {
        String encodedKey = key;
        for (final Pair<String, String> pair : ESCAPE_CHARACTERS) {
            final String unescapedCharacter = pair.getValue0();
            final String escapedCharacter = pair.getValue1();
            encodedKey = encodedKey.replace(unescapedCharacter, escapedCharacter);
        }

        return encodedKey;
    }

    /**
     * Decodes a MongoDB safe field name to an unencoded string.
     * @param key the encoded key.
     * @return the unencoded key.
     */
    public static String decodeKey(final String key) {
        String decodedKey = key;
        for (final Pair<String, String> pair : UNESCAPE_CHARACTERS) {
            final String unescapedCharacter = pair.getValue0();
            final String escapedCharacter = pair.getValue1();
            decodedKey = decodedKey.replace(escapedCharacter, unescapedCharacter);
        }

        return decodedKey;
    }
}