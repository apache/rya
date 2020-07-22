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
package org.apache.rya.api.resolver;

import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaResource;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaValue;
import org.apache.rya.api.utils.LiteralLanguageUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

/**
 * Methods for converting values from their Rya object representations into
 * their RDF4J object equivalents.
 */
public class RyaToRdfConversions {
    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    /**
     * Converts a {@link RyaIRI} into a {@link IRI} representation of the
     * {@code ryaIri}.
     * @param ryaIri the {@link RyaIRI} to convert.
     * @return the {@link IRI} representation of the {@code ryaIri}.
     */
    public static IRI convertIRI(final RyaIRI ryaIri) {
        return VF.createIRI(ryaIri.getData());
    }

    /**
     * Converts a {@link RyaValue} into a {@link Resource} representation of the
     * {@code ryaValue}.
     * @param ryaValue the {@link RyaValue} to convert.
     * @return the {@link Resource} representation of the {@code ryaValue}.
     */
    public static Resource convertResource(final RyaValue ryaValue) {
        return VF.createIRI(ryaValue.getData());
    }

    /**
     * Converts a {@link RyaValue[]} into a {@link Resource[]} representation of the
     * {@code resource}.
     * @param ryaValues the {@link RyaResource} to convert. Generally this will be
     * the subject.
     * @return the {@link Resource} representation of the {@code ryaValues}.
     */
    public static Resource[] convertResource(final RyaValue... ryaValues) {
        if (ryaValues == null || ryaValues.length == 0) {
            return null;
        }
        Resource[] resources = new Resource[ryaValues.length];
        for (int i = 0; i < ryaValues.length; i++) {
            resources[i] = convertResource(ryaValues[i]);
        }
        return resources;
    }

    /**
     * Converts a {@link RyaValue} into a {@link Literal} representation of the
     * {@code ryaValue}.
     * @param ryaValue the {@link RyaValue} to convert.
     * @return the {@link Literal} representation of the {@code ryaValue}.
     */
    public static Literal convertLiteral(final RyaValue ryaValue) {
        if (ryaValue == null) {
            return null;
        }
        RyaType ryaType = (RyaType) ryaValue;
        if (XMLSchema.STRING.equals(ryaType.getDataType())) {
            return VF.createLiteral(ryaType.getData());
        } else if (RDF.LANGSTRING.equals(ryaType.getDataType())) {
            final String data = ryaType.getData();
            final String language = ryaType.getLanguage();
            if (language != null && Literals.isValidLanguageTag(language)) {
                return VF.createLiteral(data, language);
            } else {
                return VF.createLiteral(data, LiteralLanguageUtils.UNDETERMINED_LANGUAGE);
            }
        }
        return VF.createLiteral(ryaType.getData(), ryaType.getDataType());
    }

    /**
     * Converts a {@link RyaValue} into a {@link Value} representation of the
     * {@code ryaValue}.
     * @param ryaValue the {@link RyaValue} to convert.
     * @return the {@link Value} representation of the {@code ryaType}.
     */
    public static Value convertValue(final RyaValue ryaValue) {
        if (ryaValue == null) {
            return null;
        }
        // We need to ensure IRIs stay as IRIs. The data type check here is just defensive programming and shouldn't be relied on.
        if (ryaValue instanceof RyaResource || XMLSchema.ANYURI.equals(ryaValue.getDataType())) {
            return convertResource(ryaValue);
        }
        return convertLiteral(ryaValue);
    }

    /**
     * Converts a {@link RyaStatement} into a {@link Statement} representation
     * of the {@code ryaStatement}.
     * @param ryaStatement the {@link RyaStatement} to convert.
     * @return the {@link Statement} representation of the {@code ryaStatement}.
     */
    public static Statement convertStatement(final RyaStatement ryaStatement) {
        assert ryaStatement != null;
        return VF.createStatement(
                convertResource(ryaStatement.getSubject()),
                convertIRI(ryaStatement.getPredicate()),
                convertValue(ryaStatement.getObject()),
                ryaStatement.getContext() != null ? convertResource(ryaStatement.getContext()) : null
        );
    }
}
