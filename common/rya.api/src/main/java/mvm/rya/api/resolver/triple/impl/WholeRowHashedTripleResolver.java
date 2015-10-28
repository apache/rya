package mvm.rya.api.resolver.triple.impl;

/*
 * #%L
 * mvm.rya.rya.api
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.primitives.Bytes;

import mvm.rya.api.domain.RyaStatement;
import mvm.rya.api.domain.RyaType;
import mvm.rya.api.domain.RyaURI;
import mvm.rya.api.resolver.RyaContext;
import mvm.rya.api.resolver.RyaTypeResolverException;
import mvm.rya.api.resolver.triple.TripleRow;
import mvm.rya.api.resolver.triple.TripleRowResolver;
import mvm.rya.api.resolver.triple.TripleRowResolverException;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static mvm.rya.api.RdfCloudTripleStoreConstants.*;

/**
 * Will store triple in spo, po, osp. Storing everything in the whole row.
 * Date: 7/13/12
 * Time: 8:51 AM
 */
public class WholeRowHashedTripleResolver implements TripleRowResolver {

    @Override
    public Map<TABLE_LAYOUT, TripleRow> serialize(RyaStatement stmt) throws TripleRowResolverException {
        try {
            RyaURI subject = stmt.getSubject();
            RyaURI predicate = stmt.getPredicate();
            RyaType object = stmt.getObject();
            RyaURI context = stmt.getContext();
            Long timestamp = stmt.getTimestamp();
            byte[] columnVisibility = stmt.getColumnVisibility();
            String qualifer = stmt.getQualifer();
            byte[] qualBytes = qualifer == null ? EMPTY_BYTES : qualifer.getBytes();
            byte[] value = stmt.getValue();
            assert subject != null && predicate != null && object != null;
            byte[] cf = (context == null) ? EMPTY_BYTES : context.getData().getBytes();
            Map<TABLE_LAYOUT, TripleRow> tripleRowMap = new HashMap<TABLE_LAYOUT, TripleRow>();
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] subjBytes = subject.getData().getBytes();
            byte[] subjHashBytes = md.digest(subjBytes);
            byte[] predBytes = predicate.getData().getBytes();
            byte[] predHashBytes = md.digest(predBytes);
            byte[][] objBytes = RyaContext.getInstance().serializeType(object);
            tripleRowMap.put(TABLE_LAYOUT.SPO,
                    new TripleRow(Bytes.concat(subjHashBytes, DELIM_BYTES, subjBytes, DELIM_BYTES,
                            predBytes, DELIM_BYTES,
                            objBytes[0], objBytes[1]), cf, qualBytes,
                            timestamp, columnVisibility, value));
            tripleRowMap.put(TABLE_LAYOUT.PO,
                    new TripleRow(Bytes.concat(predHashBytes, DELIM_BYTES, predBytes, DELIM_BYTES,
                            objBytes[0], DELIM_BYTES,
                            subjBytes, objBytes[1]), cf, qualBytes,
                            timestamp, columnVisibility, value));
            tripleRowMap.put(TABLE_LAYOUT.OSP,
                    new TripleRow(Bytes.concat(objBytes[0], DELIM_BYTES,
                            subjBytes, DELIM_BYTES,
                            predBytes, objBytes[1]), cf, qualBytes,
                            timestamp, columnVisibility, value));
            return tripleRowMap;
        } catch (RyaTypeResolverException e) {
            throw new TripleRowResolverException(e);
        } catch (NoSuchAlgorithmException e) {
        	throw new TripleRowResolverException(e);
		}
    }

    @Override
    public RyaStatement deserialize(TABLE_LAYOUT table_layout, TripleRow tripleRow) throws TripleRowResolverException {
        try {
            assert tripleRow != null && table_layout != null;
            byte[] row = tripleRow.getRow();
            
            // if it is a hashed row, ony keep the row after the hash
            if ((table_layout == TABLE_LAYOUT.SPO) || (table_layout == TABLE_LAYOUT.PO)) {
            	int hashStart = Bytes.indexOf(row, DELIM_BYTE);
            	row = Arrays.copyOfRange(row, hashStart + 1, row.length);
            }
            
            int firstIndex = Bytes.indexOf(row, DELIM_BYTE); 
            byte[] first= Arrays.copyOf(row, firstIndex);
            int secondIndex = Bytes.lastIndexOf(row, DELIM_BYTE);
            int typeIndex = Bytes.indexOf(row, TYPE_DELIM_BYTE);
            byte[] second = Arrays.copyOfRange(row, firstIndex + 1, secondIndex);
            byte[] third = Arrays.copyOfRange(row, secondIndex + 1, typeIndex);
            byte[] type = Arrays.copyOfRange(row, typeIndex, row.length);
            byte[] columnFamily = tripleRow.getColumnFamily();
            boolean contextExists = columnFamily != null && columnFamily.length > 0;
            RyaURI context = (contextExists) ? (new RyaURI(new String(columnFamily))) : null;
            byte[] columnQualifier = tripleRow.getColumnQualifier();
            String qualifier = columnQualifier != null && columnQualifier.length > 0 ? new String(columnQualifier) : null;
            Long timestamp = tripleRow.getTimestamp();
            byte[] columnVisibility = tripleRow.getColumnVisibility();
            byte[] value = tripleRow.getValue();

            switch (table_layout) {
                case SPO: {
                    byte[] obj = Bytes.concat(third, type);
                    return new RyaStatement(
                            new RyaURI(new String(first)),
                            new RyaURI(new String(second)),
                            RyaContext.getInstance().deserialize(obj),
                            context, qualifier, columnVisibility, value, timestamp);
                }
                case PO: {
                    byte[] obj = Bytes.concat(second, type);
                    return new RyaStatement(
                            new RyaURI(new String(third)),
                            new RyaURI(new String(first)),
                            RyaContext.getInstance().deserialize(obj),
                            context, qualifier, columnVisibility, value, timestamp);
                }
                case OSP: {
                    byte[] obj = Bytes.concat(first, type);
                    return new RyaStatement(
                            new RyaURI(new String(second)),
                            new RyaURI(new String(third)),
                            RyaContext.getInstance().deserialize(obj),
                            context, qualifier, columnVisibility, value, timestamp);
                }
            }
        } catch (RyaTypeResolverException e) {
            throw new TripleRowResolverException(e);
        }
        throw new TripleRowResolverException("TripleRow[" + tripleRow + "] with Table layout[" + table_layout + "] is not deserializable");
    }

}
