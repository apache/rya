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

package mvm.rya.indexing.external.tupleSet;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import mvm.rya.api.domain.RyaType;
import mvm.rya.api.resolver.RdfToRyaConversions;
import mvm.rya.indexing.external.tupleSet.PcjTables.VariableOrder;

/**
 * Converts {@link BindingSet}s to Strings and back again.
 */
public class BindingSetStringConverter {

    private static final String BINDING_DELIM = ":::";
    private static final String TYPE_DELIM = "<<~>>";

    private static final ValueFactory valueFactory = new ValueFactoryImpl();

    /**
     * Converts a {@link BindingSet} to a String. You must provide the order the
     * {@link Binding}s will be written to.
     * </p>
     * The resulting string does not include the binding names from the original
     * object, so that must be kept with the resulting String if you want to
     * convert it back to a BindingSet later.
     * </p>
     *
     * @param bindingSet - The BindingSet that will be converted. (not null)
     * @param varOrder - The order the bindings will appear in the resulting String. (not null)
     * @return A {@code String} version of {@code bindingSet} whose binding are
     *   ordered based on {@code varOrder}.
     */
    public static String toString(BindingSet bindingSet, VariableOrder varOrder) {
        checkSameVariables(bindingSet, varOrder);

        final StringBuilder bindingSetString = new StringBuilder();

        Iterator<String> it = varOrder.iterator();
        while(it.hasNext()) {
            // Add a value to the binding set.
            String varName = it.next();
            final Value value = bindingSet.getBinding(varName).getValue();
            final RyaType ryaValue = RdfToRyaConversions.convertValue(value);
            bindingSetString.append( ryaValue.getData() ).append(TYPE_DELIM).append( ryaValue.getDataType() );

            // If there are more values to add, include a delimiter between them.
            if(it.hasNext()) {
                bindingSetString.append(BINDING_DELIM);
            }
        }

        return bindingSetString.toString();
    }

    /**
     * Checks to see if the names of all the {@link Binding}s in the {@link BindingSet}
     * match the variable names in the {@link VariableOrder}.
     *
     * @param bindingSet - The binding set whose Bindings will be inspected. (not null)
     * @param varOrder - The names of the bindings that must appear in the BindingSet. (not null)
     * @throws IllegalArgumentException Indicates the number of bindings did not match
     *   the number of variables or that the binding names did not match the names
     *   of the variables.
     */
    private static void checkSameVariables(BindingSet bindingSet, VariableOrder varOrder) throws IllegalArgumentException {
        checkNotNull(bindingSet);
        checkNotNull(varOrder);

        Set<String> bindingNames = bindingSet.getBindingNames();
        List<String> varOrderList = varOrder.getVariableOrders();
        checkArgument(bindingNames.size() == varOrderList.size(), "The number of Bindings must match the length of the VariableOrder.");
        checkArgument(bindingNames.containsAll(varOrderList), "The names of the Bindings must match the variable names in VariableOrder.");
    }

    /**
     * Converts the String representation of a {@link BindingSet} as is created
     * by {@link #toString(BindingSet, VariableOrder)} back into a BindingSet.
     * <p>
     * You must provide the Binding names in the order they were written to.
     * </p>
     *
     * @param bindingSetString - The binding set values as a String. (not null)
     * @param varOrder - The order the bindings appear in the String version of
     *   the BindingSet. (not null)
     * @return A {@link BindingSet} representation of the String.
     */
    public static BindingSet fromString(final String bindingSetString, final VariableOrder varOrder) {
        checkNotNull(bindingSetString);
        checkNotNull(varOrder);

        final String[] bindingStrings = bindingSetString.split(BINDING_DELIM);
        final String[] varOrrderArr = varOrder.toArray();
        checkArgument(varOrrderArr.length == bindingStrings.length, "The number of Bindings must match the length of the VariableOrder.");

        final QueryBindingSet bindingSet = new QueryBindingSet();
        for(int i = 0; i < bindingStrings.length; i++) {
            final String name = varOrrderArr[i];
            final Value value = toValue(bindingStrings[i]);
            bindingSet.addBinding(name, value);
        }
        return bindingSet;
    }

    /**
     * Creates a {@link Value} from a String representation of it.
     *
     * @param valueString - The String representation of the value. (not null)
     * @return The {@link Value} representation of the String.
     */
    private static Value toValue(final String valueString) {
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