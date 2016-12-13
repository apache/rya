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

import static org.junit.Assert.assertEquals;

import org.apache.rya.indexing.entity.model.Type;
import org.apache.rya.indexing.entity.storage.mongo.TypeDocumentConverter;
import org.apache.rya.indexing.entity.storage.mongo.DocumentConverter.DocumentConverterException;
import org.bson.Document;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import mvm.rya.api.domain.RyaURI;

/**
 * Tests the methods of {@link TypeDocumentConverter}.
 */
public class TypeDocumentConverterTest {

    @Test
    public void toDocument() {
        // Convert a Type into a Document.
        final Type type = new Type(new RyaURI("urn:icecream"),
                ImmutableSet.<RyaURI>builder()
                    .add(new RyaURI("urn:brand"))
                    .add(new RyaURI("urn:flavor"))
                    .add(new RyaURI("urn:cost"))
                    .build());
        final Document document = new TypeDocumentConverter().toDocument(type);

        // Show the document has the correct values.
        final Document expected = new Document()
                .append(TypeDocumentConverter.ID, "urn:icecream")
                .append(TypeDocumentConverter.PROPERTY_NAMES, Lists.newArrayList("urn:brand", "urn:flavor", "urn:cost"));
        assertEquals(expected, document);
    }

    @Test
    public void fromDocument() throws DocumentConverterException {
        // Convert a Document into a Type.
        final Document document = new Document()
                .append(TypeDocumentConverter.ID, "urn:icecream")
                .append(TypeDocumentConverter.PROPERTY_NAMES, Lists.newArrayList("urn:brand", "urn:flavor", "urn:cost"));
        final Type type = new TypeDocumentConverter().fromDocument(document);

        // Show the converted value has the expected structure.
        final Type expected = new Type(new RyaURI("urn:icecream"),
                ImmutableSet.<RyaURI>builder()
                    .add(new RyaURI("urn:brand"))
                    .add(new RyaURI("urn:flavor"))
                    .add(new RyaURI("urn:cost"))
                    .build());
        assertEquals(expected, type);
    }

    @Test(expected = DocumentConverterException.class)
    public void fromDocument_noId() throws DocumentConverterException {
        // A document that does not have a Data Type ID.
        final Document document = new Document()
                .append(TypeDocumentConverter.PROPERTY_NAMES, Lists.newArrayList("urn:brand", "urn:flavor", "urn:cost"));

        // The conversion will fail.
        new TypeDocumentConverter().fromDocument(document);
    }

    @Test(expected = DocumentConverterException.class)
    public void fromDocument_noOptionalFieldNames() throws DocumentConverterException {
        // A document that does not have an Optional Field Names.
        final Document document = new Document()
                .append(TypeDocumentConverter.ID, "urn:icecream");

        // The conversion will fail.
        new TypeDocumentConverter().fromDocument(document);
    }
}