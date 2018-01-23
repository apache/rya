/**
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
package org.apache.rya.streams.querymanager.xml;

import static java.util.Objects.requireNonNull;

import java.io.Reader;
import java.net.URL;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.SAXException;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Validates and unmarshalls XML into a {@link QueryManagerConfig} object.
 */
@DefaultAnnotation(NonNull.class)
public class QueryManagerConfigUnmarshaller {

    /**
     * The path within the project's jar file where the XSD resides.
     */
    private static final String XSD_PATH = "QueryManagerConfig.xsd";

    /**
     * Validates and unmarshalls XML into a {@link QueryManagerConfig} object.
     *
     * @param xmlReader - Reads the XML that will be unmarshalled. (not null)
     * @return A {@link QueryManagerConfig} loaded with the XMLs values.
     * @throws SAXException Could not load the schema the XML will be validated against.
     * @throws JAXBException Could not unmarshal the XML into a POJO.
     */
    public static QueryManagerConfig unmarshall(final Reader xmlReader) throws JAXBException, SAXException {
        requireNonNull(xmlReader);

        // Get an input stream to the XSD file that is packaged inside of the jar.
        final URL schemaURL = ClassLoader.getSystemResource(XSD_PATH);
        if(schemaURL == null) {
            throw new RuntimeException("The Class Loader was unable to find the following resource: " + XSD_PATH);
        }

        // Get the schema for the object.
        final SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        final Schema schema = sf.newSchema(schemaURL);

        // Configure the unmarhsaller to validate using the schema.
        final JAXBContext context = JAXBContext.newInstance(QueryManagerConfig.class);
        final Unmarshaller unmarshaller = context.createUnmarshaller();
        unmarshaller.setSchema(schema);

        // Perform the unmarshal.
        return (QueryManagerConfig) unmarshaller.unmarshal(xmlReader);
    }
}