/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.indexing.entity.storage.mongo;

import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.indexing.entity.model.Entity;
import org.apache.rya.indexing.entity.model.Property;
import org.apache.rya.indexing.entity.storage.mongo.DocumentConverter.DocumentConverterException;
import org.bson.Document;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests the methods of {@link EntityDocumentConverter}.
 */
public class EntityDocumentConverterTest {

    @Test
    public void to_and_from_document() throws DocumentConverterException {
        // Convert an Entity into a Document.
        final Entity entity = Entity.builder()
                .setSubject(new RyaURI("urn:alice"))
                // Add some explicily typed properties.
                .setExplicitType(new RyaURI("urn:person"))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:age"), new RyaType(XMLSchema.INT, "30")))
                .setProperty(new RyaURI("urn:person"), new Property(new RyaURI("urn:eye"), new RyaType("blue")))
                // Add some implicitly typed properties.
                .setProperty(new RyaURI("urn:employee"), new Property(new RyaURI("urn:hours"), new RyaType(XMLSchema.INT, "40")))
                .setProperty(new RyaURI("urn:employee"), new Property(new RyaURI("urn:employer"), new RyaType("Burger Joint")))
                .build();

        final Document document = new EntityDocumentConverter().toDocument(entity);

        // Convert the Document back into an Entity.
        final Entity converted = new EntityDocumentConverter().fromDocument(document);

        // Ensure the original matches the round trip converted Entity.
        assertEquals(entity, converted);
    }
}