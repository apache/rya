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

package org.apache.rya.api.utils;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;

import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;

/**
 * This class configures XML Factories to protect against XML External Entity (XXE) attack. Configurations based on
 * information from: https://www.owasp.org/index.php/XML_External_Entity_(XXE)_Prevention_Cheat_Sheet
 */
public class XmlFactoryConfiguration {

    /**
     * Hardens the provided factory to protect against an XML External Entity (XXE) attack.
     *
     * @param factory - The factory to be modified.
     */
    public static void harden(final XMLInputFactory factory) {
        // From: https://www.owasp.org/index.php/XML_External_Entity_(XXE)_Prevention_Cheat_Sheet
        // To protect a Java XMLInputFactory from XXE, do this:
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false); // This disables DTDs entirely for that factory
        factory.setProperty("javax.xml.stream.isSupportingExternalEntities", false); // disable external entities
    }

    /**
     * Hardens the provided factory to protect against an XML External Entity (XXE) attack.
     *
     * @param factory - The factory to be modified.
     * @throws SAXNotRecognizedException
     * @throws SAXNotSupportedException
     * @throws ParserConfigurationException
     */
    public static void harden(final SAXParserFactory factory)
            throws SAXNotRecognizedException, SAXNotSupportedException, ParserConfigurationException {
        // From: https://www.owasp.org/index.php/XML_External_Entity_(XXE)_Prevention_Cheat_Sheet
        // To protect a Java SAXParserFactory from XXE, do this:

        // This is the PRIMARY defense. If DTDs (doctypes) are disallowed, almost all XML entity attacks are prevented
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);

        // If you can't completely disable DTDs, then at least do the following:
        // Xerces 1 - http://xerces.apache.org/xerces-j/features.html#external-general-entities
        // Xerces 2 - http://xerces.apache.org/xerces2-j/features.html#external-general-entities
        // JDK7+ - http://xml.org/sax/features/external-general-entities
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);

        // Xerces 1 - http://xerces.apache.org/xerces-j/features.html#external-parameter-entities
        // Xerces 2 - http://xerces.apache.org/xerces2-j/features.html#external-parameter-entities
        // JDK7+ - http://xml.org/sax/features/external-parameter-entities
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);

        // Disable external DTDs as well
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

        // and these as well, per Timothy Morgan's 2014 paper: "XML Schema, DTD, and Entity Attacks" (see reference
        // below)
        factory.setXIncludeAware(false);
    }

    /**
     * Hardens the provided factory to protect against an XML External Entity (XXE) attack.
     *
     * @param factory - The factory to be modified.
     * @throws ParserConfigurationException
     */
    public static void harden(final DocumentBuilderFactory factory) throws ParserConfigurationException {
        // From: https://www.owasp.org/index.php/XML_External_Entity_(XXE)_Prevention_Cheat_Sheet
        // To protect a Java DocumentBuilderFactory from XXE, do this:

        // This is the PRIMARY defense. If DTDs (doctypes) are disallowed, almost all XML entity attacks are prevented
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);

        // If you can't completely disable DTDs, then at least do the following:
        // Xerces 1 - http://xerces.apache.org/xerces-j/features.html#external-general-entities
        // Xerces 2 - http://xerces.apache.org/xerces2-j/features.html#external-general-entities
        // JDK7+ - http://xml.org/sax/features/external-general-entities
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);

        // Xerces 1 - http://xerces.apache.org/xerces-j/features.html#external-parameter-entities
        // Xerces 2 - http://xerces.apache.org/xerces2-j/features.html#external-parameter-entities
        // JDK7+ - http://xml.org/sax/features/external-parameter-entities
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);

        // Disable external DTDs as well
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);

        // and these as well, per Timothy Morgan's 2014 paper: "XML Schema, DTD, and Entity Attacks" (see reference
        // below)
        factory.setXIncludeAware(false);
        factory.setExpandEntityReferences(false);
    }
}
