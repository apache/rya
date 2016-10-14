package org.apache.rya.indexing.accumulo.entity;

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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import org.junit.Test;
import org.openrdf.model.vocabulary.XMLSchema;

import com.google.common.primitives.Bytes;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.RyaContext;
import org.apache.rya.api.resolver.RyaTypeResolverException;

public class EntityCentricIndexTest {
    private static RyaStatement ryaStatement;
    private static Key subjectCentricKey;
    private static Key objectCentricKey;
    private static Value value;

    @BeforeClass
    public static void init() throws RyaTypeResolverException {
        String subjectStr = ":subject";
        String predicateStr = ":predicate";
        RyaType object = new RyaType(XMLSchema.INTEGER, "3");
        byte[][] objectBytes = RyaContext.getInstance().serializeType(object);
        String contextStr = "http://example.org/graph";
        // no qualifier since entity-centric index doesn't store an actual column qualifier
        long timestamp = (long) 123456789;
        byte[] visibilityBytes = "test_visibility".getBytes();
        byte[] valueBytes = "test_value".getBytes();
        subjectCentricKey = new Key(
                subjectStr.getBytes(),
                predicateStr.getBytes(),
                Bytes.concat(contextStr.getBytes(),
                        DELIM_BYTES, "object".getBytes(),
                        DELIM_BYTES, objectBytes[0], objectBytes[1]),
                visibilityBytes, timestamp);
        objectCentricKey = new Key(
                objectBytes[0],
                predicateStr.getBytes(),
                Bytes.concat(contextStr.getBytes(),
                        DELIM_BYTES, "subject".getBytes(),
                        DELIM_BYTES, subjectStr.getBytes(), objectBytes[1]),
                visibilityBytes, timestamp);
        ryaStatement = new RyaStatement(
                new RyaURI(subjectStr),
                new RyaURI(predicateStr),
                new RyaType(XMLSchema.INTEGER, "3"),
                new RyaURI(contextStr),
                null, visibilityBytes, valueBytes, timestamp);
        value = new Value(valueBytes);
    }

    private static Mutation createMutationFromKeyValue(Key key, Value value) {
        Mutation m = new Mutation(key.getRow());
        m.put(key.getColumnFamily(), key.getColumnQualifier(),
                new ColumnVisibility(key.getColumnVisibility()), key.getTimestamp(), value);
        return m;
    }

    @Test
    public void testSerializeStatement() throws RyaTypeResolverException {
        Collection<Mutation> indexMutations = EntityCentricIndex.createMutations(ryaStatement);
        Assert.assertEquals("Serialization should produce two rows: subject-centric and object-centric.",
                2, indexMutations.size());
        Assert.assertTrue("Serialization of RyaStatement failed to create equivalent subject-centric row.",
                indexMutations.contains(createMutationFromKeyValue(subjectCentricKey, value)));
        Assert.assertTrue("Serialization of RyaStatement failed to create equivalent object-centric row.",
                indexMutations.contains(createMutationFromKeyValue(objectCentricKey, value)));
    }

    @Test
    public void testDeserializeSubjectRow() throws RyaTypeResolverException, IOException {
        RyaStatement deserialized = EntityCentricIndex.deserializeStatement(subjectCentricKey, value);
        Assert.assertEquals("Deserialization of subject-centric row failed to produce equivalent RyaStatement.",
                ryaStatement, deserialized);
    }

    @Test
    public void testDeserializeObjectRow() throws RyaTypeResolverException, IOException {
        RyaStatement deserialized = EntityCentricIndex.deserializeStatement(objectCentricKey, value);
        Assert.assertEquals("Deserialization of object-centric row failed to produce equivalent RyaStatement.",
                ryaStatement, deserialized);
    }
}
