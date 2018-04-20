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
package org.apache.rya.api.query.strategy.wholerow;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.DELIM_BYTES;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.LAST_BYTES;
import static org.apache.rya.api.RdfCloudTripleStoreConstants.TYPE_DELIM_BYTES;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.RdfCloudTripleStoreUtils;
import org.apache.rya.api.domain.RyaRange;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.query.strategy.AbstractTriplePatternStrategy;
import org.apache.rya.api.query.strategy.ByteRange;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTypeResolverException;

import com.google.common.primitives.Bytes;

/**
 * Date: 7/14/12
 * Time: 7:35 AM
 */
public class PoWholeRowTriplePatternStrategy extends AbstractTriplePatternStrategy {

    @Override
    public RdfCloudTripleStoreConstants.TABLE_LAYOUT getLayout() {
        return RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO;
    }

    @Override
    public Map.Entry<RdfCloudTripleStoreConstants.TABLE_LAYOUT,
            ByteRange> defineRange(final RyaIRI subject, final RyaIRI predicate, final RyaType object,
                                   final RyaIRI context, final RdfCloudTripleStoreConfiguration conf) throws IOException {
        try {
            //po(ng)
            //po_r(s)(ng)
            //p(ng)
            //p_r(o)(ng)
            //r(p)(ng)
            if (!handles(subject, predicate, object, context)) {
                return null;
            }

            final RyaContext ryaContext = RyaContext.getInstance();

            final RdfCloudTripleStoreConstants.TABLE_LAYOUT table_layout = RdfCloudTripleStoreConstants.TABLE_LAYOUT.PO;
            byte[] start, stop;
            if (object != null) {
                if (object instanceof RyaRange) {
                    //p_r(o)
                    RyaRange rv = (RyaRange) object;
                    rv = ryaContext.transformRange(rv);
                    final byte[] objStartBytes = ryaContext.serializeType(rv.getStart())[0];
                    final byte[] objEndBytes = ryaContext.serializeType(rv.getStop())[0];
                    final byte[] predBytes = predicate.getData().getBytes(StandardCharsets.UTF_8);
                    start = Bytes.concat(predBytes, DELIM_BYTES, objStartBytes);
                    stop = Bytes.concat(predBytes, DELIM_BYTES, objEndBytes, DELIM_BYTES, LAST_BYTES);
                } else {
                    if (subject != null && subject instanceof RyaRange) {
                        //po_r(s)
                        RyaRange ru = (RyaRange) subject;
                        ru = ryaContext.transformRange(ru);
                        final byte[] subjStartBytes = ru.getStart().getData().getBytes(StandardCharsets.UTF_8);
                        final byte[] subjStopBytes = ru.getStop().getData().getBytes(StandardCharsets.UTF_8);
                        final byte[] predBytes = predicate.getData().getBytes(StandardCharsets.UTF_8);
                        final byte[] objBytes = ryaContext.serializeType(object)[0];
                        start = Bytes.concat(predBytes, DELIM_BYTES, objBytes, DELIM_BYTES, subjStartBytes);
                        stop = Bytes.concat(predBytes, DELIM_BYTES, objBytes, DELIM_BYTES, subjStopBytes, TYPE_DELIM_BYTES, LAST_BYTES);
                    } else {
                        //po
                        //TODO: There must be a better way than creating multiple byte[]
                        final byte[] objBytes = ryaContext.serializeType(object)[0];
                        start = Bytes.concat(predicate.getData().getBytes(StandardCharsets.UTF_8), DELIM_BYTES, objBytes, DELIM_BYTES);
                        stop = Bytes.concat(start, LAST_BYTES);
                    }
                }
            } else if (predicate instanceof RyaRange) {
                //r(p)
                RyaRange rv = (RyaRange) predicate;
                rv = ryaContext.transformRange(rv);
                start = rv.getStart().getData().getBytes(StandardCharsets.UTF_8);
                stop = Bytes.concat(rv.getStop().getData().getBytes(StandardCharsets.UTF_8), DELIM_BYTES, LAST_BYTES);
            } else {
                //p
                start = Bytes.concat(predicate.getData().getBytes(StandardCharsets.UTF_8), DELIM_BYTES);
                stop = Bytes.concat(start, LAST_BYTES);
            }
            return new RdfCloudTripleStoreUtils.CustomEntry<RdfCloudTripleStoreConstants.TABLE_LAYOUT,
                    ByteRange>(table_layout, new ByteRange(start, stop));
        } catch (final RyaTypeResolverException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean handles(final RyaIRI subject, final RyaIRI predicate, final RyaType object, final RyaIRI context) {
        //po(ng)
        //p_r(o)(ng)
        //po_r(s)(ng)
        //p(ng)
        //r(p)(ng)
        if (predicate == null) {
            return false;
        }
        if (subject != null && !(subject instanceof RyaRange)) {
            return false;
        }
        if (predicate instanceof RyaRange) {
            return object == null && subject == null;
        }
        return subject == null || object != null;
    }
}
