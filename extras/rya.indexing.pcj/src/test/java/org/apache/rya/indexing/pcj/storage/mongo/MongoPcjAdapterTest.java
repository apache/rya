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
package org.apache.rya.indexing.pcj.storage.mongo;

import static org.junit.Assert.assertEquals;

import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.bson.conversions.Bson;
import org.junit.Test;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.impl.MapBindingSet;

public class MongoPcjAdapterTest {
    @Test
    public void serialize_bindingsSubsetOfVarOrder() throws BindingSetConversionException {
        // Setup the Binding Set.
        final MapBindingSet originalBindingSet = new MapBindingSet();
        final RyaURI uri = RdfToRyaConversions.convertURI(new URIImpl("http://a"));

        originalBindingSet.addBinding("x", new URIImpl("http://a"));
        originalBindingSet.addBinding("y", new URIImpl("http://b"));

        // Setup the variable order.
        final VariableOrder varOrder = new VariableOrder("x", "a", "y", "b");

        // Create the byte[] representation of the BindingSet.
        final BindingSetConverter<Bson> converter = new MongoPcjAdapter();
        final Bson serialized = converter.convert(originalBindingSet, varOrder);

        // Deserialize the byte[] back into the binding set.
        final BindingSet deserialized = converter.convert(serialized, varOrder);

        // Ensure the deserialized value matches the original.
        assertEquals(originalBindingSet, deserialized);
    }
}
