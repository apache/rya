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

package org.apache.rya.indexing.pcj.storage.accumulo;

import static org.junit.Assert.assertEquals;

import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSetSerDe;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.impl.MapBindingSet;

/**
 * Tests the methods of {@link VisibilityBindingSetSerDe}.
 */
public class VisibilityBindingSetSerDeTest {

    @Test
    public void rountTrip() throws Exception {
        final ValueFactory vf = new ValueFactoryImpl();

        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("name", vf.createLiteral("Alice"));
        bs.addBinding("age", vf.createLiteral(5));
        final VisibilityBindingSet original = new VisibilityBindingSet(bs, "u");

        final VisibilityBindingSetSerDe serde = new VisibilityBindingSetSerDe();
        final Bytes bytes = serde.serialize(original);
        final VisibilityBindingSet result = serde.deserialize(bytes);

        assertEquals(original, result);
    }
}
