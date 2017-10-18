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
package org.apache.rya.api.resolver.impl;

import java.nio.charset.StandardCharsets;

import com.google.common.primitives.Bytes;
import org.apache.rya.api.domain.RyaRange;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.resolver.RyaTypeResolver;
import org.apache.rya.api.resolver.RyaTypeResolverException;
import org.calrissian.mango.types.LexiTypeEncoders;
import org.calrissian.mango.types.TypeEncoder;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

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
    protected IRI dataType;
    protected byte[] markerBytes;

    public RyaTypeResolverImpl() {
        this((byte) PLAIN_LITERAL_MARKER, XMLSchema.STRING);
    }

    public RyaTypeResolverImpl(final byte markerByte, final IRI dataType) {
        setMarkerByte(markerByte);
        setRyaDataType(dataType);
    }

    public void setMarkerByte(final byte markerByte) {
        this.markerByte = markerByte;
        this.markerBytes = new byte[]{markerByte};
    }

    @Override
    public byte getMarkerByte() {
        return markerByte;
    }

    @Override
    public RyaRange transformRange(final RyaRange ryaRange) throws RyaTypeResolverException {
        return ryaRange;
    }

    @Override
    public byte[] serialize(final RyaType ryaType) throws RyaTypeResolverException {
        final byte[][] bytes = serializeType(ryaType);
        return Bytes.concat(bytes[0], bytes[1]);
    }

    @Override
    public byte[][] serializeType(final RyaType ryaType) throws RyaTypeResolverException {
        final byte[] bytes = serializeData(ryaType.getData()).getBytes(StandardCharsets.UTF_8);
        return new byte[][]{bytes, Bytes.concat(TYPE_DELIM_BYTES, markerBytes)};
    }

    @Override
    public IRI getRyaDataType() {
        return dataType;
    }

    public void setRyaDataType(final IRI dataType) {
        this.dataType = dataType;
    }

    @Override
    public RyaType newInstance() {
        return new RyaType();
    }

    @Override
    public boolean deserializable(final byte[] bytes) {
        return bytes != null && bytes.length >= 2 && bytes[bytes.length - 1] == getMarkerByte() && bytes[bytes.length - 2] == TYPE_DELIM_BYTE;
    }

    protected String serializeData(final String data) throws RyaTypeResolverException {
        return STRING_TYPE_ENCODER.encode(data);
    }

    @Override
    public RyaType deserialize(final byte[] bytes) throws RyaTypeResolverException {
        if (!deserializable(bytes)) {
            throw new RyaTypeResolverException("Bytes not deserializable");
        }
        final RyaType rt = newInstance();
        rt.setDataType(getRyaDataType());
        final String data = new String(bytes, 0, bytes.length - 2, StandardCharsets.UTF_8);
        rt.setData(deserializeData(data));
        return rt;
    }

    protected String deserializeData(final String data) throws RyaTypeResolverException {
        return STRING_TYPE_ENCODER.decode(data);
    }
}
