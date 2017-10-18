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

import org.apache.rya.api.domain.RyaSchema;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.VarNameUtils;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.StatementPattern;
import org.eclipse.rdf4j.query.algebra.Var;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Contains method that convert between the RDF4J representations of RDF
 * components and the Strings that are used by the Fluo PCJ application.
 */
@DefaultAnnotation(NonNull.class)
public class FluoStringConverter {

    /**
     * Extract the {@link BindingSet} strings from a {@link BindingSet}'s string form.
     *
     * @param bindingSetString - A {@link BindingSet} in its Fluo String form. (not null)
     * @return The set's {@link BindingSet}s in Fluo String form. (not null)
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
     * @return A {@link StatementPattern} built from the string.
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
        final String[] varParts = varString.split(TYPE_DELIM);
        final String name = varParts[0];
        final ValueFactory vf = SimpleValueFactory.getInstance();

        // The variable is a constant value.
        if(varParts.length > 1) {
            final String dataTypeString = varParts[1];
            if(dataTypeString.equals(URI_TYPE)) {
                // Handle a URI object.
                Preconditions.checkArgument(varParts.length == 2);
                final String valueString = VarNameUtils.removeConstant(name);
                final Var var = new Var(name, vf.createIRI(valueString));
                var.setConstant(true);
                return var;
            } else if(dataTypeString.equals(RyaSchema.BNODE_NAMESPACE)) {
                // Handle a BNode object
                Preconditions.checkArgument(varParts.length == 3);
                final Var var = new Var(name);
                var.setValue(vf.createBNode(varParts[2]));
                return var;
            } else {
                // Handle a Literal Value.
                Preconditions.checkArgument(varParts.length == 2);
                final String valueString = VarNameUtils.removeConstant(name);
                final IRI dataType = vf.createIRI(dataTypeString);
                final Literal value = vf.createLiteral(valueString, dataType);
                final Var var = new Var(name, value);
                var.setConstant(true);
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
        if(subjVar.getValue() != null) {
            final Value subjValue = subjVar.getValue();
            subj = VarNameUtils.createSimpleConstVarName(subjVar);
            if (subjValue instanceof BNode ) {
                subj = subj + TYPE_DELIM + RyaSchema.BNODE_NAMESPACE + TYPE_DELIM + ((BNode) subjValue).getID();
            } else {
                subj = subj + TYPE_DELIM + URI_TYPE;
            }
        }

        final Var predVar = sp.getPredicateVar();
        String pred = predVar.getName();
        if(predVar.getValue() != null) {
            pred = VarNameUtils.createSimpleConstVarName(predVar);
            pred = pred + TYPE_DELIM + URI_TYPE;
        }

        final Var objVar = sp.getObjectVar();
        String obj = objVar.getName();
        if (objVar.getValue() != null) {
            final Value objValue = objVar.getValue();
            obj = VarNameUtils.createSimpleConstVarName(objVar);
            final RyaType rt = RdfToRyaConversions.convertValue(objValue);
            obj =  obj + TYPE_DELIM + rt.getDataType().stringValue();
        }

        return subj + DELIM + pred + DELIM + obj;
    }
}