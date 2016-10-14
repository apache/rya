package org.apache.rya.api.query.strategy.wholerow;

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



import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTES;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.LAST_BYTES;

import java.io.IOException;
import java.util.Map;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaRange;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.query.strategy.AbstractTriplePatternStrategy;
import org.apache.rya.api.query.strategy.ByteRange;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTypeResolverException;

import com.google.common.primitives.Bytes;

/**
 * Date: 7/14/12
 * Time: 7:35 AM
 */
public class OspWholeRowTriplePatternStrategy extends AbstractTriplePatternStrategy {

    @Override
    public TABLE_LAYOUT getLayout() {
        return TABLE_LAYOUT.OSP;
    }

    @Override
    public Map.Entry<TABLE_LAYOUT,
            ByteRange> defineRange(RyaURI subject, RyaURI predicate, RyaType object,
                                   RyaURI context, RdfCloudTripleStoreConfiguration conf) throws IOException {
        try {
            //os(ng)
            //o_r(s)(ng)
            //o(ng)
            //r(o)
            if (!handles(subject, predicate, object, context)) return null;

            RyaContext ryaContext = RyaContext.getInstance();

            TABLE_LAYOUT table_layout = TABLE_LAYOUT.OSP;
            byte[] start, stop;
            if (subject != null) {
                if (subject instanceof RyaRange) {
                    //o_r(s)
                    RyaRange ru = (RyaRange) subject;
                    ru = ryaContext.transformRange(ru);
                    byte[] subjStartBytes = ru.getStart().getData().getBytes();
                    byte[] subjEndBytes = ru.getStop().getData().getBytes();
                    byte[] objBytes = ryaContext.serializeType(object)[0];
                    start = Bytes.concat(objBytes, DELIM_BYTES, subjStartBytes);
                    stop = Bytes.concat(objBytes, DELIM_BYTES, subjEndBytes, DELIM_BYTES, LAST_BYTES);
                } else {
                    //os
                    byte[] objBytes = ryaContext.serializeType(object)[0];
                    start = Bytes.concat(objBytes, DELIM_BYTES, subject.getData().getBytes(), DELIM_BYTES);
                    stop = Bytes.concat(start, LAST_BYTES);
                }
            } else {
                if (object instanceof RyaRange) {
                    //r(o)
                    RyaRange rv = (RyaRange) object;
                    rv = ryaContext.transformRange(rv);
                    start = ryaContext.serializeType(rv.getStart())[0];
                    stop = Bytes.concat(ryaContext.serializeType(rv.getStop())[0], DELIM_BYTES, LAST_BYTES);
                } else {
                    //o
                    start = Bytes.concat(ryaContext.serializeType(object)[0], DELIM_BYTES);
                    stop = Bytes.concat(start, LAST_BYTES);
                }
            }
            return new RdfCloudTripleStoreUtils.CustomEntry<TABLE_LAYOUT,
                    ByteRange>(table_layout, new ByteRange(start, stop));
        } catch (RyaTypeResolverException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean handles(RyaURI subject, RyaURI predicate, RyaType object, RyaURI context) {
        //os(ng)
        //o_r(s)(ng)
        //o(ng)
        //r(o)
        return object != null && (!(object instanceof RyaRange) || predicate == null && subject == null);
    }
}
