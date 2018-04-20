package org.apache.rya.api.query.strategy.wholerow;

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

import static org.apache.rya.api.RdfCloudTripleStoreConstants.LAST_BYTES;

import java.io.IOException;
import java.util.Map;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.query.strategy.AbstractTriplePatternStrategy;
import org.apache.rya.api.query.strategy.ByteRange;

public class NullRowTriplePatternStrategy extends AbstractTriplePatternStrategy {

    @Override
    public TABLE_LAYOUT getLayout() {
        return TABLE_LAYOUT.SPO;
    }

    @Override
    public Map.Entry<TABLE_LAYOUT, ByteRange> defineRange(RyaIRI subject, RyaIRI predicate, RyaType object,
                                                          RyaIRI context, RdfCloudTripleStoreConfiguration conf) throws IOException {
      byte[] start = new byte[]{ /* empty array */ }; // Scan from the beginning of the Accumulo Table
      byte[] stop = LAST_BYTES;  // Scan to the end, up through things beginning with 0xff.
      return new RdfCloudTripleStoreUtils.CustomEntry<>(TABLE_LAYOUT.SPO, new ByteRange(start, stop));
    }

    @Override
    public boolean handles(RyaIRI subject, RyaIRI predicate, RyaType object, RyaIRI context) {
        return subject == null && predicate == null && object == null;
    }
}
