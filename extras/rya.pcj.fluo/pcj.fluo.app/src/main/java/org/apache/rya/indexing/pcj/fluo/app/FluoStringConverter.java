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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.TYPE_DELIM;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.URI_TYPE;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.VAR_DELIM;

import java.util.Collection;

import javax.annotation.ParametersAreNonnullByDefault;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.base.Joiner;

import mvm.rya.api.domain.RyaType;
import mvm.rya.api.resolver.RdfToRyaConversions;

/**
 * Contains method that convert between the Sesame representations of RDF
 * components and the Strings that are used by the Fluo PCJ application.
 */
@ParametersAreNonnullByDefault
public class FluoStringConverter {

    private static final ValueFactory valueFactory = new ValueFactoryImpl();

    /**
     * Converts an ordered collection of variables into the Variable Order
     * String that is stored in the {@link IncrementalUpdateConstants#NODE_VARS}
     * column of the Fluo application.
     *
     * @param varOrder - An ordered collection of variables. (not null)
     * @return The string representation of the variable order.
     */
    public static String toVarOrderString(final Collection<String> varOrder) {
        checkNotNull(varOrder);
        return Joiner.on(VAR_DELIM).join(varOrder);
    }

    /**
     * Converts an ordered array of variables into the Variable Order
     * String that is stored in the {@link IncrementalUpdateConstants#NODE_VARS}
     * column of the Fluo application.
     *
     * @param varOrder - An ordered array of variables. (not null)
     * @return The string representation of the variable order.
     */
    public static String toVarOrderString(final String... varOrder) {
        return Joiner.on(VAR_DELIM).join(varOrder);
    }

    /**
     * Converts a String into an array holding the Variable Order of a Binding Set.
     *
     * @param varOrderString - The string representation of the variable order. (not null)
     * @return An ordered array holding the variable order of a binding set.
     */
    public static String[] toVarOrder(final String varOrderString) {
        checkNotNull(varOrderString);
        return varOrderString.split(VAR_DELIM);
    }

    /**
     * Converts a {@link BindingSet} to the String representation that the Fluo
     * application serializes to the Binding Set columns.
     *
     * @param bindingSet - The binding set values. (not null)
     * @param varOrder - The order the variables must appear in. (not null)
     * @return A {@code String} version of {@code bindingSet} suitable for
     *   serialization to one of the Fluo application's binding set columns.
     */
    public static String toBindingSetString(final BindingSet bindingSet, final String[] varOrder) {
        checkNotNull(bindingSet);
        checkNotNull(varOrder);

        final StringBuilder bindingSetString = new StringBuilder();

        for(int i = 0; i < varOrder.length; i++) {
            // Add a value to the binding set.
            final String varName = varOrder[i];
            final Value value = bindingSet.getBinding(varName).getValue();
            final RyaType ryaValue = RdfToRyaConversions.convertValue(value);
            bindingSetString.append( ryaValue.getData() ).append(TYPE_DELIM).append( ryaValue.getDataType() );

            // If there are more values to add, include a delimiter between them.
            if(i != varOrder.length-1) {
                bindingSetString.append(DELIM);
            }
        }

        return bindingSetString.toString();
    }

    /**
     * Converts the String representation of a {@link BindingSet} as is created
     * by {@link #toBindingSetString(BindingSet, String[])} back into a
     * BindingSet.
     *
     * @param bindingSetString - The binding set values as a String. (not null)
     * @param varOrder - The order the variables appear in the String version of
     *   the BindingSet. (not null)
     * @return A {@link BindingSet} representation of the String.
     */
    public static BindingSet toBindingSet(final String bindingSetString, final String[] varOrder) {
        checkNotNull(bindingSetString);
        checkNotNull(varOrder);

        final String[] bindingStrings = toBindingStrings(bindingSetString);
        return toBindingSet(bindingStrings, varOrder);
    }

    /**
     * Creates a {@link BindingSet} from an ordered array of Strings that represent
     * {@link Binding}s and their variable names.
     *
     * @param bindingStrings - An ordered array of Strings representing {@link Binding}s. (not null)
     * @param varOrder - An ordered array of variable names for the binding strings. (not null)
     * @return The parameters converted into a {@link BindingSet}.
     */
    public static BindingSet toBindingSet(final String[] bindingStrings, final String[] varOrder) {
        checkNotNull(varOrder);
        checkNotNull(bindingStrings);
        checkArgument(varOrder.length == bindingStrings.length);

        final QueryBindingSet bindingSet = new QueryBindingSet();

        for(int i = 0; i < bindingStrings.length; i++) {
            final String name = varOrder[i];
            final Value value = FluoStringConverter.toValue(bindingStrings[i]);
            bindingSet.addBinding(name, value);
        }

        return bindingSet;
    }

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
     * Creates a {@link Value} from a String representation of it.
     *
     * @param valueString - The String representation of the value. (not null)
     * @return The {@link Value} representation of the String.
     */
    public static Value toValue(final String valueString) {
        checkNotNull(valueString);

        // Split the String that was stored in Fluo into its Value and Type parts.
        final String[] valueAndType = valueString.split(TYPE_DELIM);
        if(valueAndType.length != 2) {
            throw new IllegalArgumentException("Array must contain data and type info!");
        }

        final String dataString = valueAndType[0];
        final String typeString = valueAndType[1];

        // Convert the String Type into a URI that describes the type.
        final URI typeURI = valueFactory.createURI(typeString);

        // Convert the String Value into a Value.
        final Value value = typeURI.equals(XMLSchema.ANYURI) ?
                valueFactory.createURI(dataString) :
                valueFactory.createLiteral(dataString, new URIImpl(typeString));

        return value;
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