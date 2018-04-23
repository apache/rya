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
package org.apache.rya.rdftriplestore.utils;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.eclipse.rdf4j.common.lang.FileFormat;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParserRegistry;

import com.google.common.collect.ImmutableSet;

/**
 * Utility methods for {@link RDFFormat}.
 */
public final class RdfFormatUtils {
    /**
     * Holds all supported {@link RDFFormat} types.
     */
    public static final Set<RDFFormat> RDF_FORMATS = RDFParserRegistry.getInstance().getKeys();

    /**
     * The set of all supported file extensions from {@link #RDF_FORMATS}.
     */
    public static final Set<String> SUPPORTED_FILE_EXTENSIONS = buildSupportedFileExtensions();

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

    /**
     * Tries to determine the appropriate RDF file format based on the extension
     * of a file name.
     * @param fileName A file name.
     * @return An {@link RDFFormat} object if the file extension was recognized,
     * or {@code null} otherwise.
     */
    public static RDFFormat forFileName(final String fileName) {
        return forFileName(fileName, null);
    }

    /**
     * Tries to determine the appropriate RDF file format based on the extension
     * of a file name. The supplied fallback format will be returned when the
     * file name extension was not recognized.
     * @param fileName A file name.
     * @return An {@link RDFFormat} that matches the file name extension, or the
     * fallback format if the extension was not recognized.
     */
    public static RDFFormat forFileName(final String fileName, final RDFFormat fallback) {
        final Optional<RDFFormat> match = FileFormat.matchFileName(fileName, RDF_FORMATS);
        if (match.isPresent()) {
            return match.get();
        } else {
            return fallback;
        }
    }

    /**
     * @return the set of all supported file extensions from
     * {@link #RDF_FORMATS}.
     */
    private static Set<String> buildSupportedFileExtensions() {
        return ImmutableSet.copyOf(RDF_FORMATS.stream().flatMap(rdfFormat -> rdfFormat.getFileExtensions().stream()).collect(Collectors.toList()));
    }
}