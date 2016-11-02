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
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;

/**
 * Tests the methods of {@link DocumentVisibilityAdapter}.
 */
public class DocumentVisibilityAdapterTest {
    @Test
    public void testToDBObject() {
        final DocumentVisibility dv = new DocumentVisibility("A");
        final BasicDBObject dbObject = DocumentVisibilityAdapter.toDBObject(dv);
        final BasicDBObject expected = (BasicDBObject) JSON.parse(
            "{" +
                "documentVisibility : [[\"A\"]]" +
            "}"
        );
        assertEquals(expected, dbObject);
    }

    @Test
    public void testToDBObject_and() {
        final DocumentVisibility dv = new DocumentVisibility("A&B&C");
        final BasicDBObject dbObject = DocumentVisibilityAdapter.toDBObject(dv);
        final BasicDBObject expected = (BasicDBObject) JSON.parse(
            "{" +
                "documentVisibility : [[\"A\", \"B\", \"C\"]]" +
            "}"
        );
        assertEquals(expected, dbObject);
    }

    @Test
    public void testToDBObject_or() {
        final DocumentVisibility dv = new DocumentVisibility("A|B|C");
        final BasicDBObject dbObject = DocumentVisibilityAdapter.toDBObject(dv);
        final BasicDBObject expected = (BasicDBObject) JSON.parse(
            "{" +
                "documentVisibility : [[\"C\"], [\"B\"], [\"A\"]]" +
            "}"
        );
        assertEquals(expected, dbObject);
    }

    @Test
    public void testToDBObject_Expression() {
        final DocumentVisibility dv = new DocumentVisibility("A&B&C");
        final BasicDBObject dbObject = DocumentVisibilityAdapter.toDBObject(dv.getExpression());
        final BasicDBObject expected = (BasicDBObject) JSON.parse(
            "{" +
                "documentVisibility : [[\"A\", \"B\", \"C\"]]" +
            "}"
        );
        assertEquals(expected, dbObject);
    }

    @Test
    public void testToDBObject_nullExpression() {
        final BasicDBObject dbObject = DocumentVisibilityAdapter.toDBObject((byte[])null);
        final BasicDBObject expected = (BasicDBObject) JSON.parse(
            "{" +
                "documentVisibility : []" +
            "}"
        );
        assertEquals(expected, dbObject);
    }

    @Test
    public void testToDBObject_nullDocumentVisibility() {
        final BasicDBObject dbObject = DocumentVisibilityAdapter.toDBObject((DocumentVisibility)null);
        final BasicDBObject expected = (BasicDBObject) JSON.parse(
            "{" +
                "documentVisibility : []" +
            "}"
        );
        assertEquals(expected, dbObject);
    }

    @Test
    public void testToDBObject_emptyDocumentVisibility() {
        final BasicDBObject dbObject = DocumentVisibilityAdapter.toDBObject(MongoDbRdfConstants.EMPTY_DV);
        final BasicDBObject expected = (BasicDBObject) JSON.parse(
            "{" +
                "documentVisibility : []" +
            "}"
        );
        assertEquals(expected, dbObject);
    }

    @Test
    public void testToDocumentVisibility() throws MalformedDocumentVisibilityException {
        final BasicDBObject dbObject = (BasicDBObject) JSON.parse(
            "{" +
                "documentVisibility : [\"A\"]" +
            "}"
        );
        final DocumentVisibility dv = DocumentVisibilityAdapter.toDocumentVisibility(dbObject);
        final DocumentVisibility expected = new DocumentVisibility("A");
        assertEquals(expected, dv);
    }

    @Test
    public void testToDocumentVisibility_and() throws MalformedDocumentVisibilityException {
        final BasicDBObject dbObject = (BasicDBObject) JSON.parse(
            "{" +
                "documentVisibility : [\"A\", \"B\", \"C\"]" +
            "}"
        );
        final DocumentVisibility dv = DocumentVisibilityAdapter.toDocumentVisibility(dbObject);
        final DocumentVisibility expected = new DocumentVisibility("A&B&C");
        assertEquals(expected, dv);
    }

    @Test
    public void testToDocumentVisibility_or() throws MalformedDocumentVisibilityException {
        final BasicDBObject dbObject = (BasicDBObject) JSON.parse(
            "{" +
                "documentVisibility : [[\"A\"], [\"B\"], [\"C\"]]" +
            "}"
        );
        final DocumentVisibility dv = DocumentVisibilityAdapter.toDocumentVisibility(dbObject);
        final DocumentVisibility expected = new DocumentVisibility("A|B|C");
        assertEquals(expected, dv);
    }

    @Test
    public void testToDocumentVisibility_empty() throws MalformedDocumentVisibilityException {
        final BasicDBObject dbObject = (BasicDBObject) JSON.parse(
            "{" +
                "documentVisibility : []" +
            "}"
        );
        final DocumentVisibility dv = DocumentVisibilityAdapter.toDocumentVisibility(dbObject);
        final DocumentVisibility expected = MongoDbRdfConstants.EMPTY_DV;
        assertEquals(expected, dv);
    }

    @Test (expected=MalformedDocumentVisibilityException.class)
    public void testToDocumentVisibility_null() throws MalformedDocumentVisibilityException {
        DocumentVisibilityAdapter.toDocumentVisibility(null);
    }
}