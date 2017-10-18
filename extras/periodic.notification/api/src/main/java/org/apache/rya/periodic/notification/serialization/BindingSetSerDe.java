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
package org.apache.rya.periodic.notification.serialization;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.primitives.Bytes;

import com.google.common.base.Joiner;
import com.google.common.primitives.Bytes;

/**
 * Kafka {@link Serializer} and {@link Deserializer} for producing and consuming messages
 * from Kafka.
 *
 */
public class BindingSetSerDe implements Serializer<BindingSet>, Deserializer<BindingSet> {

    private static final Logger log = LoggerFactory.getLogger(BindingSetSerDe.class);
    private static final AccumuloPcjSerializer serializer =  new AccumuloPcjSerializer();
    private static final byte[] DELIM_BYTE = "\u0002".getBytes(StandardCharsets.UTF_8);

    private byte[] toBytes(final BindingSet bindingSet) {
        try {
            return getBytes(getVarOrder(bindingSet), bindingSet);
        } catch(final Exception e) {
            log.trace("Unable to serialize BindingSet: " + bindingSet);
            return new byte[0];
        }
    }

    private BindingSet fromBytes(final byte[] bsBytes) {
        try{
            final int firstIndex = Bytes.indexOf(bsBytes, DELIM_BYTE);
            final byte[] varOrderBytes = Arrays.copyOf(bsBytes, firstIndex);
            final byte[] bsBytesNoVarOrder = Arrays.copyOfRange(bsBytes, firstIndex + 1, bsBytes.length);
            final VariableOrder varOrder = new VariableOrder(new String(varOrderBytes, StandardCharsets.UTF_8).split(";"));
            return getBindingSet(varOrder, bsBytesNoVarOrder);
        } catch(final Exception e) {
            log.trace("Unable to deserialize BindingSet: " + bsBytes);
            return new QueryBindingSet();
        }
    }

    private VariableOrder getVarOrder(final BindingSet bs) {
        return new VariableOrder(bs.getBindingNames());
    }

    private byte[] getBytes(final VariableOrder varOrder, final BindingSet bs) throws UnsupportedEncodingException, BindingSetConversionException {
        final byte[] bsBytes = serializer.convert(bs, varOrder);
        final String varOrderString = Joiner.on(";").join(varOrder.getVariableOrders());
        final byte[] varOrderBytes = varOrderString.getBytes(StandardCharsets.UTF_8);
        return Bytes.concat(varOrderBytes, DELIM_BYTE, bsBytes);
    }

    private BindingSet getBindingSet(final VariableOrder varOrder, final byte[] bsBytes) throws BindingSetConversionException {
        return serializer.convert(bsBytes, varOrder);
    }

    @Override
    public BindingSet deserialize(final String topic, final byte[] bytes) {
        return fromBytes(bytes);
    }

    @Override
    public void close() {
        // Do nothing. Nothing to close.
    }

    @Override
    public void configure(final Map<String, ?> arg0, final boolean arg1) {
        // Do nothing.  Nothing to configure.
    }

    @Override
    public byte[] serialize(final String topic, final BindingSet bs) {
        return toBytes(bs);
    }

}
