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

import java.util.Set;

import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParserRegistry;

/**
 * Utility methods for {@link RDFFormat}.
 */
public final class RdfFormatUtils {
    private static final Set<RDFFormat> RDF_FORMATS = RDFParserRegistry.getInstance().getKeys();

    /**
     * Private constructor to prevent instantiation.
     */
    private RdfFormatUtils() {
    }

    /**
     * Gets the RDF format whose name matches the specified name.
     * @param formatName The format name.
     * @return The {@link RDFFormat} whose name matches the specified name, or
     * {@code null} if there is no such format.
     */
    public static RDFFormat getRdfFormatFromName(final String formatName) {
        for (final RDFFormat rdfFormat : RDF_FORMATS) {
            if (rdfFormat.getName().equalsIgnoreCase(formatName)) {
                return rdfFormat;
            }
        }
        return null;
    }
}