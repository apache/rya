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
package org.apache.rya.streams.kafka.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.kafka.common.serialization.Serde;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Test;

/**
 * Tests the methods of {@link VisibilityBindingSetSerde}.
 */
public class VisibilityBindingSetSerdeTest {

    @Test
    public void serializeAndDeserialize() {
        // Create the object that will be serialized.
        final ValueFactory vf = SimpleValueFactory.getInstance();
        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("name", vf.createLiteral("alice"));
        bs.addBinding("age", vf.createLiteral(37));

        final VisibilityBindingSet original  = new VisibilityBindingSet(bs, "a|b|c");

        // Serialize it.
        try(final Serde<VisibilityBindingSet> serde = new VisibilityBindingSetSerde()) {
            final byte[] bytes = serde.serializer().serialize("topic", original);

            // Deserialize it.
            final VisibilityBindingSet deserialized = serde.deserializer().deserialize("topic", bytes);

            // Show the deserialized value matches the original.
            assertEquals(original, deserialized);
        }
    }

    @Test
    public void deserializeEmptyData() {
        try(final Serde<VisibilityBindingSet> serde = new VisibilityBindingSetSerde()) {
            assertNull( serde.deserializer().deserialize("topic", new byte[0]) );
        }
    }
}