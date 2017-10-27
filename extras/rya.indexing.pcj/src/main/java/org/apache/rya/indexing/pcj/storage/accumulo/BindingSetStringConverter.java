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
package org.apache.rya.indexing.pcj.storage.accumulo;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.impl.MapBindingSet;

import com.google.common.base.Joiner;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Converts {@link BindingSet}s to Strings and back again. The Strings do not
 * include the binding names and are ordered with a {@link VariableOrder}.
 */
@DefaultAnnotation(NonNull.class)
public class BindingSetStringConverter implements BindingSetConverter<String> {

    public static final String BINDING_DELIM = ":::";
    public static final String TYPE_DELIM = "<<~>>";
    public static final String NULL_VALUE_STRING = Character.toString( '\0' );

    private static final ValueFactory VF = SimpleValueFactory.getInstance();

    @Override
    public String convert(final BindingSet bindingSet, final VariableOrder varOrder) {
        requireNonNull(bindingSet);
        requireNonNull(varOrder);

        // Convert each Binding to a String.
        final List<String> bindingStrings = new ArrayList<>();
        for(final String varName : varOrder) {
            if(bindingSet.hasBinding(varName)) {
                // Add a value to the binding set.
                final Value value = bindingSet.getBinding(varName).getValue();
                final RyaType ryaValue = RdfToRyaConversions.convertValue(value);
                final String bindingString = ryaValue.getData() + TYPE_DELIM + ryaValue.getDataType();
                bindingStrings.add(bindingString);
            } else {
                // Add a null value to the binding set.
                bindingStrings.add(NULL_VALUE_STRING);
            }
        }

        // Join the bindings using the binding delim.
        return Joiner.on(BINDING_DELIM).join(bindingStrings);
    }

    @Override
    public BindingSet convert(final String bindingSetString, final VariableOrder varOrder) {
        requireNonNull(bindingSetString);
        requireNonNull(varOrder);

        // If both are empty, return an empty binding set.
        if(bindingSetString.isEmpty() && varOrder.toString().isEmpty()) {
            return new MapBindingSet();
        }

        // Otherwise parse it.
        final String[] bindingStrings = bindingSetString.split(BINDING_DELIM);
        final String[] varOrderArr = varOrder.toArray();
        checkArgument(varOrderArr.length == bindingStrings.length, "The number of Bindings must match the length of the VariableOrder.");

        final QueryBindingSet bindingSet = new QueryBindingSet();
        for(int i = 0; i < bindingStrings.length; i++) {
            final String bindingString = bindingStrings[i];
            if(!NULL_VALUE_STRING.equals(bindingString)) {
                final String name = varOrderArr[i];
                final Value value = toValue(bindingStrings[i]);
                bindingSet.addBinding(name, value);
            }
        }
        return bindingSet;
    }

    /**
     * Creates a {@link Value} from a String representation of it.
     *
     * @param valueString - The String representation of the value. (not null)
     * @return The {@link Value} representation of the String.
     */
    protected static Value toValue(final String valueString) {
        requireNonNull(valueString);

        // Split the String that was stored in Fluo into its Value and Type parts.
        final String[] valueAndType = valueString.split(TYPE_DELIM);
        if(valueAndType.length != 2) {
            throw new IllegalArgumentException("Array must contain data and type info!");
        }

        final String dataString = valueAndType[0];
        final String typeString = valueAndType[1];

        // Convert the String Type into a URI that describes the type.
        final IRI typeURI = VF.createIRI(typeString);

        // Convert the String Value into a Value.
        final Value value = typeURI.equals(XMLSchema.ANYURI) ?
                VF.createIRI(dataString) :
                VF.createLiteral(dataString, VF.createIRI(typeString));

        return value;
    }
}