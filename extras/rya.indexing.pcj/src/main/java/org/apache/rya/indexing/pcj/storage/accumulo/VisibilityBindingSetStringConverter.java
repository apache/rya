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

import javax.annotation.ParametersAreNonnullByDefault;

import org.openrdf.query.BindingSet;

/**
 * Converts {@link BindingSet}s to Strings and back again. The Strings do not
 * include the binding names and are ordered with a {@link VariableOrder}.
 */
@ParametersAreNonnullByDefault
public class VisibilityBindingSetStringConverter extends BindingSetStringConverter {
    public static final char VISIBILITY_DELIM = 1;

    private static final int BINDING_SET_STRING_INDEX = 0;
    private static final int VISIBILITY_EXPRESSION_INDEX = 1;

    @Override
    public String convert(final BindingSet bindingSet, final VariableOrder varOrder) {
        // Convert the BindingSet into its String format.
        String bindingSetString = super.convert(bindingSet, varOrder);

        // Append the visibilities if they are present.
        if(bindingSet instanceof VisibilityBindingSet) {
            final String visibility = ((VisibilityBindingSet) bindingSet).getVisibility();
            if(!visibility.isEmpty()) {
                bindingSetString += VISIBILITY_DELIM + visibility;
            }
        }

        return bindingSetString;
    }

    @Override
    public VisibilityBindingSet convert(final String bindingSetString, final VariableOrder varOrder) {
        // Try to split the binding set string over the visibility delimiter.
        final String[] strings = bindingSetString.split("" + VISIBILITY_DELIM);

        // Convert the binding set string into a BindingSet.
        final BindingSet bindingSet = super.convert(strings[BINDING_SET_STRING_INDEX], varOrder);

        // If a visibility expression is present, then also include it.
        return (strings.length > 1) ?
                new VisibilityBindingSet(bindingSet, strings[VISIBILITY_EXPRESSION_INDEX]) :
                new VisibilityBindingSet(bindingSet);
    }
}