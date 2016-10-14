package org.apache.rya.api.resolver.impl;

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



import com.google.common.primitives.Bytes;
import org.apache.rya.api.domain.RyaRange;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RyaTypeResolver;
import org.apache.rya.api.resolver.RyaTypeResolverException;
import org.calrissian.mango.types.LexiTypeEncoders;
import org.calrissian.mango.types.TypeEncoder;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.XMLSchema;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.TYPE_DELIM_BYTE;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.TYPE_DELIM_BYTES;

/**
 * Date: 7/16/12
 * Time: 12:42 PM
 */
public class RyaTypeResolverImpl implements RyaTypeResolver {
    public static final int PLAIN_LITERAL_MARKER = 3;
    public static final TypeEncoder<String, String> STRING_TYPE_ENCODER = LexiTypeEncoders
            .stringEncoder();

    protected byte markerByte;
    protected URI dataType;
    protected byte[] markerBytes;

    public RyaTypeResolverImpl() {
        this((byte) PLAIN_LITERAL_MARKER, XMLSchema.STRING);
    }

    public RyaTypeResolverImpl(byte markerByte, URI dataType) {
        setMarkerByte(markerByte);
        setRyaDataType(dataType);
    }

    public void setMarkerByte(byte markerByte) {
        this.markerByte = markerByte;
        this.markerBytes = new byte[]{markerByte};
    }

    @Override
    public byte getMarkerByte() {
        return markerByte;
    }

    @Override
    public RyaRange transformRange(RyaRange ryaRange) throws RyaTypeResolverException {
        return ryaRange;
    }

    @Override
    public byte[] serialize(RyaType ryaType) throws RyaTypeResolverException {
        byte[][] bytes = serializeType(ryaType);
        return Bytes.concat(bytes[0], bytes[1]);
    }

    @Override
    public byte[][] serializeType(RyaType ryaType) throws RyaTypeResolverException {
        byte[] bytes = serializeData(ryaType.getData()).getBytes();
        return new byte[][]{bytes, Bytes.concat(TYPE_DELIM_BYTES, markerBytes)};
    }

    @Override
    public URI getRyaDataType() {
        return dataType;
    }

    public void setRyaDataType(URI dataType) {
        this.dataType = dataType;
    }

    @Override
    public RyaType newInstance() {
        return new RyaType();
    }

    @Override
    public boolean deserializable(byte[] bytes) {
        return bytes != null && bytes.length >= 2 && bytes[bytes.length - 1] == getMarkerByte() && bytes[bytes.length - 2] == TYPE_DELIM_BYTE;
    }

    protected String serializeData(String data) throws RyaTypeResolverException {
        return STRING_TYPE_ENCODER.encode(data);
    }

    @Override
    public RyaType deserialize(byte[] bytes) throws RyaTypeResolverException {
        if (!deserializable(bytes)) {
            throw new RyaTypeResolverException("Bytes not deserializable");
        }
        RyaType rt = newInstance();
        rt.setDataType(getRyaDataType());
        String data = new String(bytes, 0, bytes.length - 2);
        rt.setData(deserializeData(data));
        return rt;
    }

    protected String deserializeData(String data) throws RyaTypeResolverException {
        return STRING_TYPE_ENCODER.decode(data);
    }
}
