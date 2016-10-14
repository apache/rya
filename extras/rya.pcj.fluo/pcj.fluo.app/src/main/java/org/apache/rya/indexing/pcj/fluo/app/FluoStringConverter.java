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
package org.apache.rya.indexing.pcj.fluo.app;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.TYPE_DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.URI_TYPE;

import javax.annotation.ParametersAreNonnullByDefault;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RdfToRyaConversions;

/**
 * Contains method that convert between the Sesame representations of RDF
 * components and the Strings that are used by the Fluo PCJ application.
 */
@ParametersAreNonnullByDefault
public class FluoStringConverter {

    /**
     * Extract the {@link Binding} strings from a {@link BindingSet}'s string form.
     *
     * @param bindingSetString - A {@link BindingSet} in its Fluo String form. (not null)
     * @return The set's {@link Binding}s in Fluo String form. (not null)
     */
    public static String[] toBindingStrings(final String bindingSetString) {
        checkNotNull(bindingSetString);
        return bindingSetString.split(DELIM);
    }

    /**
     * Converts the String representation of a {@link StatementPattern} back
     * into the object version.
     *
     * @param patternString - The {@link StatementPattern} represented as a String. (not null)
     * @return A {@link StatementPatter} built from the string.
     */
    public static StatementPattern toStatementPattern(final String patternString) {
        checkNotNull(patternString);

        final String[] parts = patternString.split(DELIM);
        final String subjectPart = parts[0];
        final String predicatePart = parts[1];
        final String objectPart = parts[2];

        final Var subject = toVar(subjectPart);
        final Var predicate = toVar(predicatePart);
        final Var object = toVar(objectPart);

        return new StatementPattern(subject, predicate, object);
    }

    /**
     * Converts the String representation of a {@link Var} back into the object version.
     *
     * @param varString - The {@link Var} represented as a String. (not null)
     * @return A {@link Var} built from the string.
     */
    public static Var toVar(final String varString) {
        checkNotNull(varString);

        if(varString.startsWith("-const-")) {
            // The variable is a constant value.
            final String[] varParts = varString.split(TYPE_DELIM);
            final String name = varParts[0];
            final String valueString = name.substring("-const-".length());

            final String dataTypeString = varParts[1];
            if(dataTypeString.equals(URI_TYPE)) {
                // Handle a URI object.
                final Var var = new Var(name, new URIImpl(valueString));
                var.setAnonymous(true);
                return var;
            } else {
                // Literal value.
                final URI dataType = new URIImpl(dataTypeString);
                final Literal value = new LiteralImpl(valueString, dataType);
                final Var var = new Var(name, value);
                var.setAnonymous(true);
                return var;
            }
        } else {
            // The variable is a named variable.
            return new Var(varString);
        }
    }

    /**
     * Provides a string representation of an SP which contains info about
     * whether each component (subj, pred, obj) is constant and its data and
     * data type if it is constant.
     *
     * @param sp - The statement pattern to convert. (not null)
     * @return A String representation of the statement pattern that may be
     *   used to do triple matching.
     */
    public static String toStatementPatternString(final StatementPattern sp) {
        checkNotNull(sp);

        final Var subjVar = sp.getSubjectVar();
        String subj = subjVar.getName();
        if(subjVar.isConstant()) {
            subj = subj + TYPE_DELIM + URI_TYPE;
        }

        final Var predVar = sp.getPredicateVar();
        String pred = predVar.getName();
        if(predVar.isConstant()) {
            pred = pred + TYPE_DELIM + URI_TYPE;
        }

        final Var objVar = sp.getObjectVar();
        String obj = objVar.getName();
        if (objVar.isConstant()) {
            final RyaType rt = RdfToRyaConversions.convertValue(objVar.getValue());
            obj =  obj + TYPE_DELIM + rt.getDataType().stringValue();
        }

        return subj + DELIM + pred + DELIM + obj;
    }
}