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



import com.google.common.primitives.Bytes;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaRange;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.domain.RyaURIRange;
import org.apache.rya.api.query.strategy.AbstractTriplePatternStrategy;
import org.apache.rya.api.query.strategy.ByteRange;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTypeResolverException;

import java.io.IOException;
import java.util.Map;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.*;

/**
 * Date: 7/14/12
 * Time: 7:35 AM
 */
public class SpoWholeRowTriplePatternStrategy extends AbstractTriplePatternStrategy {

    @Override
    public TABLE_LAYOUT getLayout() {
        return TABLE_LAYOUT.SPO;
    }

    @Override
    public Map.Entry<TABLE_LAYOUT, ByteRange> defineRange(RyaURI subject, RyaURI predicate, RyaType object,
                                                          RyaURI context, RdfCloudTripleStoreConfiguration conf) throws IOException {
        try {
            //spo(ng)
            //sp(ng)
            //s(ng)
            //sp_r(o)(ng)
            //s_r(p)(ng)
            if (!handles(subject, predicate, object, context)) return null;

            RyaContext ryaContext = RyaContext.getInstance();

            TABLE_LAYOUT table_layout = TABLE_LAYOUT.SPO;
            byte[] start;
            byte[] stop;
            if (predicate != null) {
                if (object != null) {
                    if (object instanceof RyaRange) {
                        //sp_r(o)
                        //range = sp_r(o.s)->sp_r(o.e) (remove last byte to remove type info)
                        RyaRange rv = (RyaRange) object;
                        rv = ryaContext.transformRange(rv);
                        byte[] objStartBytes = ryaContext.serializeType(rv.getStart())[0];
                        byte[] objEndBytes = ryaContext.serializeType(rv.getStop())[0];
                        byte[] subjBytes = subject.getData().getBytes();
                        byte[] predBytes = predicate.getData().getBytes();
                        start = Bytes.concat(subjBytes, DELIM_BYTES, predBytes, DELIM_BYTES, objStartBytes);
                        stop = Bytes.concat(subjBytes, DELIM_BYTES, predBytes, DELIM_BYTES, objEndBytes, DELIM_BYTES, LAST_BYTES);
                    } else {
                        //spo
                        //range = spo->spo (remove last byte to remove type info)
                        //TODO: There must be a better way than creating multiple byte[]
                        byte[] objBytes = ryaContext.serializeType(object)[0];
                        start = Bytes.concat(subject.getData().getBytes(), DELIM_BYTES, predicate.getData().getBytes(), DELIM_BYTES, objBytes, TYPE_DELIM_BYTES);
                        stop = Bytes.concat(start, LAST_BYTES);
                    }
                } else if (predicate instanceof RyaRange) {
                    //s_r(p)
                    //range = s_r(p.s)->s_r(p.e)
                    RyaRange rv = (RyaRange) predicate;
                    rv = ryaContext.transformRange(rv);
                    byte[] subjBytes = subject.getData().getBytes();
                    byte[] predStartBytes = rv.getStart().getData().getBytes();
                    byte[] predStopBytes = rv.getStop().getData().getBytes();
                    start = Bytes.concat(subjBytes, DELIM_BYTES, predStartBytes);
                    stop = Bytes.concat(subjBytes, DELIM_BYTES, predStopBytes, DELIM_BYTES, LAST_BYTES);
                } else {
                    //sp
                    //range = sp
                    start = Bytes.concat(subject.getData().getBytes(), DELIM_BYTES, predicate.getData().getBytes(), DELIM_BYTES);
                    stop = Bytes.concat(start, LAST_BYTES);
                }
            } else if (subject instanceof RyaRange) {
                //r(s)
                //range = r(s.s) -> r(s.e)
                RyaRange ru = (RyaRange) subject;
                ru = ryaContext.transformRange(ru);
                start = ru.getStart().getData().getBytes();
                stop = Bytes.concat(ru.getStop().getData().getBytes(), DELIM_BYTES, LAST_BYTES);
            } else {
                //s
                //range = s
                start = Bytes.concat(subject.getData().getBytes(), DELIM_BYTES);
                stop = Bytes.concat(start, LAST_BYTES);
            }
            return new RdfCloudTripleStoreUtils.CustomEntry<TABLE_LAYOUT, ByteRange>(table_layout,
                    new ByteRange(start, stop));
        } catch (RyaTypeResolverException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean handles(RyaURI subject, RyaURI predicate, RyaType object, RyaURI context) {
        //if subject is not null and (if predicate is null then object must be null)
        return (subject != null && !(subject instanceof RyaURIRange && predicate != null)) && !((predicate == null || predicate instanceof RyaURIRange) && (object != null));
    }
}
