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
import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTE;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTES;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.TYPE_DELIM_BYTE;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.annotation.ParametersAreNonnullByDefault;

import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.primitives.Bytes;

import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaToRdfConversions;
import org.apache.rya.api.resolver.RyaTypeResolverException;

/**
 * Converts {@link BindingSet}s to byte[]s and back again. The bytes do not
 * include the binding names and are ordered with a {@link VariableOrder}.
 */
@ParametersAreNonnullByDefault
public class AccumuloPcjSerializer implements BindingSetConverter<byte[]> {

    @Override
    public byte[] convert(BindingSet bindingSet, VariableOrder varOrder) throws BindingSetConversionException {
        checkNotNull(bindingSet);
        checkNotNull(varOrder);
        checkBindingsSubsetOfVarOrder(bindingSet, varOrder);

        // A list that holds all of the byte segments that will be concatenated at the end.
        // This minimizes byte[] construction.
        final List<byte[]> byteSegments = new LinkedList<>();

        try {
            for(final String varName: varOrder) {
                // Only write information for a variable name if the binding set contains it.
                if(bindingSet.hasBinding(varName)) {
                    final RyaType rt = RdfToRyaConversions.convertValue(bindingSet.getBinding(varName).getValue());
                    final byte[][] serializedVal = RyaContext.getInstance().serializeType(rt);
                    byteSegments.add(serializedVal[0]);
                    byteSegments.add(serializedVal[1]);
                }

                // But always write the value delimiter. If a value is missing, you'll see two delimiters next to each-other.
                byteSegments.add(DELIM_BYTES);
            }

            return concat(byteSegments);
        } catch (RyaTypeResolverException e) {
            throw new BindingSetConversionException("Could not convert the BindingSet into a byte[].", e);
        }
    }

    @Override
    public BindingSet convert(byte[] bindingSetBytes, VariableOrder varOrder) throws BindingSetConversionException {
        checkNotNull(bindingSetBytes);
        checkNotNull(varOrder);

        try {
            // Slice the row into bindings.
            List<byte[]> values = splitlByDelimByte(bindingSetBytes);
            String[] varOrderStrings = varOrder.toArray();
            checkArgument(values.size() == varOrderStrings.length);

            // Convert the Binding bytes into a BindingSet.
            final QueryBindingSet bindingSet = new QueryBindingSet();

            for(int i = 0; i < varOrderStrings.length; i++) {
                byte[] valueBytes = values.get(i);
                if(valueBytes.length > 0) {
                    String name = varOrderStrings[i];
                    Value value = deserializeValue(valueBytes);
                    bindingSet.addBinding(name, value);
                }
            }

            return bindingSet;
        } catch (RyaTypeResolverException e) {
            throw new BindingSetConversionException("Could not convert the byte[] into a BindingSet.", e);
        }
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
    private static void checkBindingsSubsetOfVarOrder(BindingSet bindingSet, VariableOrder varOrder) throws IllegalArgumentException {
        checkNotNull(bindingSet);
        checkNotNull(varOrder);

        Set<String> bindingNames = bindingSet.getBindingNames();
        List<String> varNames = varOrder.getVariableOrders();
        checkArgument(varNames.containsAll(bindingNames), "The BindingSet contains a Binding whose name is not part of the VariableOrder.");
    }

    private static final byte[] concat(Iterable<byte[]> byteSegments) {
        checkNotNull(byteSegments);

        // Allocate a byte array that is able to hold the segments.
        int length = 0;
        for(byte[] byteSegment : byteSegments) {
            length += byteSegment.length;
        }
        byte[] result = new byte[length];

        // Copy the segments to the byte array and return it.
        ByteBuffer buff = ByteBuffer.wrap(result);
        for(byte[] byteSegment : byteSegments) {
            buff.put(byteSegment);
        }
        return result;
    }

    private static List<byte[]> splitlByDelimByte(byte[] bindingSetBytes) {
        checkNotNull(bindingSetBytes);

        List<byte[]> values = new LinkedList<>();

        ByteBuffer buff = ByteBuffer.wrap(bindingSetBytes);
        int start = 0;
        while(buff.hasRemaining()) {
            if(buff.get() == DELIM_BYTE) {
                // Mark the position of the value delimiter.
                int end = buff.position();

                // Move to the start of the value and copy the bytes into an array.
                byte[] valueBytes = new byte[(end - start) -1];
                buff.position(start);
                buff.get(valueBytes);
                buff.position(end);
                values.add(valueBytes);

                // Move the start of the next value to the end of this one.
                start = end;
            }
        }

        return values;
    }

    private static Value deserializeValue(byte[] byteVal) throws RyaTypeResolverException {
         final int typeIndex = Bytes.indexOf(byteVal, TYPE_DELIM_BYTE);
         checkArgument(typeIndex >= 0);
         final byte[] data = Arrays.copyOf(byteVal, typeIndex);
         final byte[] type = Arrays.copyOfRange(byteVal, typeIndex, byteVal.length);
         final RyaType rt = RyaContext.getInstance().deserialize(Bytes.concat(data,type));
         return RyaToRdfConversions.convertValue(rt);
    }
}