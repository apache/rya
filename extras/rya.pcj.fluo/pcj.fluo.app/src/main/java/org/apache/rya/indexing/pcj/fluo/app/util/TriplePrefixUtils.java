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
package org.apache.rya.indexing.pcj.fluo.app.util;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.NODEID_BS_DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.TRIPLE_PREFIX;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.apache.rya.api.domain.RyaStatement;

/**
 * This class is a utility class for adding and removing the Triple prefix to
 * Statements ingested into the Rya Fluo application.  The Triple prefix is added
 * to supported range based compactions for removing transient data from the Fluo
 * application.  This prefix supports the Transient data recipe described on the
 * the Fluo website, and reduces the computational load on the system by cleaning up
 * old deleted Triples and notifications using a targeted range compaction.
 *
 */
public class TriplePrefixUtils {

    private static final Bytes TRIPLE_PREFIX_BYTES = Bytes.of(TRIPLE_PREFIX + NODEID_BS_DELIM);

    /**
     * Prepends the triple prefix to the provided bytes and returns the new value as a {@link Bytes}.
     * @param tripleBytes - serialized {@link RyaStatement}
     * @return - serialized RyaStatement with prepended triple prefix, converted to Bytes
     */
    public static Bytes addTriplePrefixAndConvertToBytes(byte[] tripleBytes) {
        checkNotNull(tripleBytes);
        BytesBuilder builder = Bytes.builder();
        return builder.append(TRIPLE_PREFIX_BYTES).append(tripleBytes).toBytes();
    }

    /**
     * Removes the triple prefix and returns the new value as a byte array.
     * @param prefixedTriple - serialized RyaStatement with prepended triple prefix, converted to Bytes
     * @return - serialized {@link RyaStatement} in byte array form
     */
    public static byte[] removeTriplePrefixAndConvertToByteArray(Bytes prefixedTriple) {
        checkNotNull(prefixedTriple);
        return prefixedTriple.subSequence(TRIPLE_PREFIX_BYTES.length(), prefixedTriple.length()).toArray();
    }
}
