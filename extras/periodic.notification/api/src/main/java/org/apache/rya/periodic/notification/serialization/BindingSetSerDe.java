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
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.log4j.Logger;
import org.apache.rya.indexing.pcj.storage.accumulo.AccumuloPcjSerializer;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetConverter.BindingSetConversionException;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.query.BindingSet;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

import com.google.common.base.Joiner;
import com.google.common.primitives.Bytes;

/**
 * Kafka {@link Serializer} and {@link Deserializer} for producing and consuming messages
 * from Kafka.
 *
 */
public class BindingSetSerDe implements Serializer<BindingSet>, Deserializer<BindingSet> {

    private static final Logger log = Logger.getLogger(BindingSetSerDe.class);
    private static final AccumuloPcjSerializer serializer =  new AccumuloPcjSerializer();
    private static final byte[] DELIM_BYTE = "\u0002".getBytes();
    
    private byte[] toBytes(BindingSet bindingSet) {
        try {
            return getBytes(getVarOrder(bindingSet), bindingSet);
        } catch(Exception e) {
            log.trace("Unable to serialize BindingSet: " + bindingSet);
            return new byte[0];
        }
    }

    private BindingSet fromBytes(byte[] bsBytes) {
        try{
        int firstIndex = Bytes.indexOf(bsBytes, DELIM_BYTE);
        byte[] varOrderBytes = Arrays.copyOf(bsBytes, firstIndex);
        byte[] bsBytesNoVarOrder = Arrays.copyOfRange(bsBytes, firstIndex + 1, bsBytes.length);
        VariableOrder varOrder = new VariableOrder(new String(varOrderBytes,"UTF-8").split(";"));
        return getBindingSet(varOrder, bsBytesNoVarOrder);
        } catch(Exception e) {
            log.trace("Unable to deserialize BindingSet: " + bsBytes);
            return new QueryBindingSet();
        }
    }
    
    private VariableOrder getVarOrder(BindingSet bs) {
        return new VariableOrder(bs.getBindingNames());
    }
    
    private byte[] getBytes(VariableOrder varOrder, BindingSet bs) throws UnsupportedEncodingException, BindingSetConversionException {
        byte[] bsBytes = serializer.convert(bs, varOrder);
        String varOrderString = Joiner.on(";").join(varOrder.getVariableOrders());
        byte[] varOrderBytes = varOrderString.getBytes("UTF-8");
        return Bytes.concat(varOrderBytes, DELIM_BYTE, bsBytes);
    }
    
    private BindingSet getBindingSet(VariableOrder varOrder, byte[] bsBytes) throws BindingSetConversionException {
        return serializer.convert(bsBytes, varOrder);
    }

    @Override
    public BindingSet deserialize(String topic, byte[] bytes) {
        return fromBytes(bytes);
    }

    @Override
    public void close() {
        // Do nothing. Nothing to close.
    }

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {
        // Do nothing.  Nothing to configure.
    }

    @Override
    public byte[] serialize(String topic, BindingSet bs) {
        return toBytes(bs);
    }

}
