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

import org.apache.log4j.Logger;
import org.apache.rya.api.domain.RangeIRI;
import org.apache.rya.api.domain.RangeValue;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaIRIRange;
import org.apache.rya.api.domain.RyaResource;
import org.apache.rya.api.domain.RyaSchema;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaTypeRange;
import org.apache.rya.api.domain.RyaValue;
import org.apache.rya.api.log.LogUtils;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.Literals;

import static org.apache.rya.api.utils.LiteralLanguageUtils.UNDETERMINED_LANGUAGE;

/**
 * Methods for converting values from their RDF4J object representations into
 * their Rya object equivalents.
 */
public class RdfToRyaConversions {
    private static final Logger log = Logger.getLogger(RdfToRyaConversions.class);

    /**
     * Converts a {@link IRI} into a {@link RyaIRI} representation of the
     * {@code iri}.
     * @param iri the {@link IRI} to convert.
     * @return the {@link RyaIRI} representation of the {@code iri}.
     */
    public static RyaIRI convertIRI(final IRI iri) {
        if (iri == null) {
            return null;
        }
        if (iri instanceof RyaIRI) {
            return (RyaIRI) iri;
        }
        if (iri instanceof RangeIRI) {
            final RangeIRI riri = (RangeIRI) iri;
            return new RyaIRIRange(convertResource(riri.getStart()), convertResource(riri.getEnd()));
        }
        return new RyaIRI(iri.stringValue());
    }

    /**
     * Converts a {@link Literal} into a {@link RyaValue} representation of the
     * {@code literal}.
     * @param literal the {@link Literal} to convert.
     * @return the {@link RyaValue} representation of the {@code literal}.
     */
    public static RyaValue convertLiteral(final Literal literal) {
        if (literal == null) {
            return null;
        }
        if (literal instanceof RyaType) {
            return (RyaType) literal;
        }
        if (literal.getDatatype() != null) {
            if (Literals.isLanguageLiteral(literal)) {
                final String language = literal.getLanguage().get();
                if (Literals.isValidLanguageTag(language)) {
                    return new RyaType(literal.getDatatype(), literal.stringValue(), language);
                } else {
                    log.warn("Invalid language (" + LogUtils.clean(language) + ") found in Literal. Defaulting to: " + UNDETERMINED_LANGUAGE);
                    // Replace invalid language with "und"
                    return new RyaType(literal.getDatatype(), literal.stringValue(), UNDETERMINED_LANGUAGE);
                }
            }
            return new RyaType(literal.getDatatype(), literal.stringValue());
        }
        return new RyaType(literal.stringValue());
    }

    /**
     * Converts a {@link Value} into a {@link RyaValue} representation of the
     * {@code value}.
     * @param value the {@link Value} to convert.
     * @return the {@link RyaValue} representation of the {@code value}.
     */
    public static RyaValue convertValue(final Value value) {
        if (value == null) {
            return null;
        }
        if (value instanceof RyaValue) {
            return (RyaValue) value;
        }
        //assuming either IRI or Literal here
        if (value instanceof Resource) {
            return convertResource((Resource) value);
        }
        if (value instanceof Literal) {
            return convertLiteral((Literal) value);
        }
        if (value instanceof RangeValue) {
            final RangeValue<?> rv = (RangeValue<?>) value;
            if (rv.getStart() instanceof IRI) {
                return new RyaIRIRange(convertIRI((IRI) rv.getStart()), convertIRI((IRI) rv.getEnd()));
            } else {
                //literal
                return new RyaTypeRange(convertLiteral((Literal) rv.getStart()), convertLiteral((Literal) rv.getEnd()));
            }
        }
        return null;
    }

    /**
     * Converts a {@link Resource} into a {@link RyaResource} representation of the
     * {@code resource}.
     * @param resource the {@link Resource} to convert. Generally this will be
     * the subject.
     * @return the {@link RyaResource} representation of the {@code resource}.
     */
    public static RyaResource convertResource(final Resource resource) {
        if (resource == null) {
            return null;
        }
        if (resource instanceof RyaIRI) {
            return (RyaIRI) resource;
        }
        if (resource instanceof BNode) {
            return new RyaIRI(RyaSchema.BNODE_NAMESPACE + ((BNode) resource).getID());
        }
        if (resource instanceof IRI) {
            return convertIRI((IRI) resource);
        }
        return (RyaResource) convertValue(resource);
    }

    /**
     * Converts a {@link Resource[]} into a {@link RyaResource[]} representation of the
     * {@code resource}.
     * @param resources the {@link Resource} to convert. Generally this will be
     * the subject.
     * @return the {@link RyaResource} representation of the {@code resources}.
     */
    public static RyaResource[] convertResource(final Resource... resources) {
        if (resources == null || resources.length == 0) {
            return null;
        }
        RyaResource[] ryaResources = new RyaResource[resources.length];
        for (int i = 0; i < resources.length; i++) {
            ryaResources[i] = convertResource(resources[i]);
        }
        return ryaResources;
    }

    /**
     * Converts a {@link Statement} into a {@link RyaStatement} representation
     * of the {@code statement}.
     * @param statement the {@link Statement} to convert.
     * @return the {@link RyaStatement} representation of the {@code statement}.
     */
    public static RyaStatement convertStatement(final Statement statement) {
        if (statement == null) {
            return null;
        }
        if (statement instanceof RyaStatement) {
            return (RyaStatement) statement;
        }
        final Resource subject = statement.getSubject();
        final IRI predicate = statement.getPredicate();
        final Value object = statement.getObject();
        final Resource context = statement.getContext();
        return new RyaStatement(
                convertResource(subject),
                convertIRI(predicate),
                convertValue(object),
                convertResource(context));
    }
}
