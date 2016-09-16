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
package mvm.rya.indexing.entity.storage.mongo;

import static org.junit.Assert.assertEquals;

import org.bson.Document;
import org.junit.Test;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;

import mvm.rya.api.domain.RyaType;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.indexing.entity.storage.mongo.DocumentConverter.DocumentConverterException;

/**
 * Tests the methods of {@link RyaTypeDocumentConverter}.
 */
public class RyaTypeDocumentConverterTest {

    @Test
    public void toDocument() {
        // Convert the RyaType into a Document.
        final RyaType ryaType = RdfToRyaConversions.convertLiteral( new ValueFactoryImpl().createLiteral( 4.5 ) );
        final Document document = new RyaTypeDocumentConverter().toDocument( ryaType );

        // Show the document has the correct structure.
        final Document expected = new Document()
                .append(RyaTypeDocumentConverter.DATA_TYPE, XMLSchema.DOUBLE.toString())
                .append(RyaTypeDocumentConverter.VALUE, "4.5");
        assertEquals(expected, document);
    }

    @Test
    public void fromDocument() throws DocumentConverterException {
        // Convert a document into a RyaType
        final Document document = new Document()
                .append(RyaTypeDocumentConverter.DATA_TYPE, XMLSchema.DOUBLE.toString())
                .append(RyaTypeDocumentConverter.VALUE, "4.5");
        final RyaType ryaType = new RyaTypeDocumentConverter().fromDocument( document );

        // Show the converted value has the expected structure.
        final RyaType expected = RdfToRyaConversions.convertLiteral( new ValueFactoryImpl().createLiteral( 4.5 ) );
        assertEquals(expected, ryaType);
    }

    @Test(expected = DocumentConverterException.class)
    public void fromDocument_noDataType() throws DocumentConverterException {
        // A document that does not have a data type.
        final Document document = new Document()
                .append(RyaTypeDocumentConverter.VALUE, "4.5");

        // The conversion will fail.
        new RyaTypeDocumentConverter().fromDocument(document);
    }

    @Test(expected = DocumentConverterException.class)
    public void fromDocument_noValue() throws DocumentConverterException {
        // A document that does not have a value.
        final Document document = new Document()
            .append(RyaTypeDocumentConverter.DATA_TYPE, XMLSchema.DOUBLE.toString());

        // The conversion will fail.
        new RyaTypeDocumentConverter().fromDocument(document);
    }
}