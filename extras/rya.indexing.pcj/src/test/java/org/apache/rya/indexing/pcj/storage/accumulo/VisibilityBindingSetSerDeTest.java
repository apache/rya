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

import java.io.ByteArrayOutputStream;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import org.apache.fluo.api.data.Bytes;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.impl.MapBindingSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests the methods of {@link VisibilityBindingSetSerDe}.
 */
public class VisibilityBindingSetSerDeTest {

    @Test
    public void rountTrip() throws Exception {
        final ValueFactory vf = SimpleValueFactory.getInstance();

        final MapBindingSet bs = new MapBindingSet();
        bs.addBinding("name", vf.createLiteral("Alice"));
        bs.addBinding("age", vf.createLiteral(5));
        final VisibilityBindingSet original = new VisibilityBindingSet(bs, "u");

        final VisibilityBindingSetSerDe serde = new VisibilityBindingSetSerDe();
        final Bytes bytes = serde.serialize(original);
        final VisibilityBindingSet result = serde.deserialize(bytes);

        assertEquals(original, result);
    }

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests that deserializing an ArrayList should throw an error.
     * if VisibilityBindingSet changes to include ArrayList, then this will need changing.
     *
     * @throws Exception
     */
    @Test
    public void rejectUnexpectedClass() throws Exception {
        // cannot use VisibilityBindingSetSerDe.serialize here since it only serializes VisibilityBindingSet.
        final ByteArrayOutputStream boas = new ByteArrayOutputStream();
        try (final ObjectOutputStream oos = new ObjectOutputStream(boas)) {
            oos.writeObject(new ArrayList<Integer>());
        }
        final Bytes bytes = Bytes.of(boas.toByteArray());
        final VisibilityBindingSetSerDe serde = new VisibilityBindingSetSerDe();
        // Should throw an InvalidClassException when deserializing the wrong class.
        exception.expect(InvalidClassException.class);
        serde.deserialize(bytes);
    }
}
