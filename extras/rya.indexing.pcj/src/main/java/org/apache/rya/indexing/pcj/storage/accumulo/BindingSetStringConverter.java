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
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.ParametersAreNonnullByDefault;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.base.Joiner;

import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RdfToRyaConversions;

/**
 * Converts {@link BindingSet}s to Strings and back again. The Strings do not
 * include the binding names and are ordered with a {@link VariableOrder}.
 */
@ParametersAreNonnullByDefault
public class BindingSetStringConverter implements BindingSetConverter<String> {

    public static final String BINDING_DELIM = ":::";
    public static final String TYPE_DELIM = "<<~>>";
    public static final String NULL_VALUE_STRING = Character.toString( '\0' );

    private static final ValueFactory valueFactory = new ValueFactoryImpl();

    @Override
    public String convert(final BindingSet bindingSet, final VariableOrder varOrder) {
        checkBindingsSubsetOfVarOrder(bindingSet, varOrder);

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

    /**
     * Checks to see if the names of all the {@link Binding}s in the {@link BindingSet}
     * are a subset of the variables names in {@link VariableOrder}.
     *
     * @param bindingSet - The binding set whose Bindings will be inspected. (not null)
     * @param varOrder - The names of the bindings that may appear in the BindingSet. (not null)
     * @throws IllegalArgumentException Indicates the names of the bindings are
     *   not a subset of the variable order.
     */
    private static void checkBindingsSubsetOfVarOrder(final BindingSet bindingSet, final VariableOrder varOrder) throws IllegalArgumentException {
        checkNotNull(bindingSet);
        checkNotNull(varOrder);

        final Set<String> bindingNames = bindingSet.getBindingNames();
        final List<String> varNames = varOrder.getVariableOrders();
        checkArgument(varNames.containsAll(bindingNames), "The BindingSet contains a Binding whose name is not part of the VariableOrder.");
    }

    @Override
    public BindingSet convert(final String bindingSetString, final VariableOrder varOrder) {
        checkNotNull(bindingSetString);
        checkNotNull(varOrder);

        final String[] bindingStrings = bindingSetString.split(BINDING_DELIM);
        final String[] varOrrderArr = varOrder.toArray();
        checkArgument(varOrrderArr.length == bindingStrings.length, "The number of Bindings must match the length of the VariableOrder.");

        final QueryBindingSet bindingSet = new QueryBindingSet();
        for(int i = 0; i < bindingStrings.length; i++) {
            final String bindingString = bindingStrings[i];
            if(!NULL_VALUE_STRING.equals(bindingString)) {
                final String name = varOrrderArr[i];
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
}