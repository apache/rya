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
package org.apache.rya.mongodb.document.visibility;

import static org.junit.Assert.assertEquals;

import org.apache.rya.mongodb.MongoDbRdfConstants;
import org.apache.rya.mongodb.document.visibility.DocumentVisibilityAdapter.MalformedDocumentVisibilityException;
import org.bson.Document;
import org.junit.Test;

/**
 * Tests the methods of {@link DocumentVisibilityAdapter}.
 */
public class DocumentVisibilityAdapterTest {
    @Test
    public void testToDocument() {
        final DocumentVisibility dv = new DocumentVisibility("A");
        final Document document = DocumentVisibilityAdapter.toDocument(dv);
        final Document expected = Document.parse(
            "{" +
                "documentVisibility : [[\"A\"]]" +
            "}"
        );
        assertEquals(expected, document);
    }

    @Test
    public void testToDocument_and() {
        final DocumentVisibility dv = new DocumentVisibility("A&B&C");
        final Document document = DocumentVisibilityAdapter.toDocument(dv);
        final Document expected = Document.parse(
            "{" +
                "documentVisibility : [[\"A\", \"B\", \"C\"]]" +
            "}"
        );
        assertEquals(expected, document);
    }

    @Test
    public void testToDocument_or() {
        final DocumentVisibility dv = new DocumentVisibility("A|B|C");
        final Document document = DocumentVisibilityAdapter.toDocument(dv);
        final Document expected = Document.parse(
            "{" +
                "documentVisibility : [[\"C\"], [\"B\"], [\"A\"]]" +
            "}"
        );
        assertEquals(expected, document);
    }

    @Test
    public void testToDocument_Expression() {
        final DocumentVisibility dv = new DocumentVisibility("A&B&C");
        final Document document = DocumentVisibilityAdapter.toDocument(dv.getExpression());
        final Document expected = Document.parse(
            "{" +
                "documentVisibility : [[\"A\", \"B\", \"C\"]]" +
            "}"
        );
        assertEquals(expected, document);
    }

    @Test
    public void testToDocument_nullExpression() {
        final Document document = DocumentVisibilityAdapter.toDocument((byte[])null);
        final Document expected = Document.parse(
            "{" +
                "documentVisibility : []" +
            "}"
        );
        assertEquals(expected, document);
    }

    @Test
    public void testToDocument_nullDocumentVisibility() {
        final Document document = DocumentVisibilityAdapter.toDocument((DocumentVisibility)null);
        final Document expected = Document.parse(
            "{" +
                "documentVisibility : []" +
            "}"
        );
        assertEquals(expected, document);
    }

    @Test
    public void testToDocument_emptyDocumentVisibility() {
        final Document document = DocumentVisibilityAdapter.toDocument(MongoDbRdfConstants.EMPTY_DV);
        final Document expected = Document.parse(
            "{" +
                "documentVisibility : []" +
            "}"
        );
        assertEquals(expected, document);
    }

    @Test
    public void testToDocumentVisibility() throws MalformedDocumentVisibilityException {
        final Document document = Document.parse(
            "{" +
                "documentVisibility : [\"A\"]" +
            "}"
        );
        final DocumentVisibility dv = DocumentVisibilityAdapter.toDocumentVisibility(document);
        final DocumentVisibility expected = new DocumentVisibility("A");
        assertEquals(expected, dv);
    }

    @Test
    public void testToDocumentVisibility_and() throws MalformedDocumentVisibilityException {
        final Document document = Document.parse(
            "{" +
                "documentVisibility : [\"A\", \"B\", \"C\"]" +
            "}"
        );
        final DocumentVisibility dv = DocumentVisibilityAdapter.toDocumentVisibility(document);
        final DocumentVisibility expected = new DocumentVisibility("A&B&C");
        assertEquals(expected, dv);
    }

    @Test
    public void testToDocumentVisibility_or() throws MalformedDocumentVisibilityException {
        final Document document = Document.parse(
            "{" +
                "documentVisibility : [[\"A\"], [\"B\"], [\"C\"]]" +
            "}"
        );
        final DocumentVisibility dv = DocumentVisibilityAdapter.toDocumentVisibility(document);
        final DocumentVisibility expected = new DocumentVisibility("A|B|C");
        assertEquals(expected, dv);
    }

    @Test
    public void testToDocumentVisibility_empty() throws MalformedDocumentVisibilityException {
        final Document document = Document.parse(
            "{" +
                "documentVisibility : []" +
            "}"
        );
        final DocumentVisibility dv = DocumentVisibilityAdapter.toDocumentVisibility(document);
        final DocumentVisibility expected = MongoDbRdfConstants.EMPTY_DV;
        assertEquals(expected, dv);
    }

    @Test (expected=MalformedDocumentVisibilityException.class)
    public void testToDocumentVisibility_null() throws MalformedDocumentVisibilityException {
        DocumentVisibilityAdapter.toDocumentVisibility(null);
    }
}