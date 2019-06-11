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
package org.apache.rya.mongodb.dao;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.mongodb.StatefulMongoDBRdfConfiguration;
import org.bson.Document;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.Namespace;

import com.mongodb.DBCollection;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

public class SimpleMongoDBNamespaceManager implements MongoDBNamespaceManager {

    public class NamespaceImplementation implements Namespace {
        private static final long serialVersionUID = 1L;

        private final String namespace;
        private final String prefix;

        public NamespaceImplementation(final String namespace, final String prefix) {
            this.namespace = namespace;
            this.prefix = prefix;
        }

        @Override
        public int compareTo(final Namespace o) {
            if (!namespace.equalsIgnoreCase(o.getName())) {
                return namespace.compareTo(o.getName());
            }
            if (!prefix.equalsIgnoreCase(o.getPrefix())) {
                return prefix.compareTo(o.getPrefix());
            }
            return 0;
        }

        @Override
        public String getName() {
            return namespace;
        }

        @Override
        public String getPrefix() {
            return prefix;
        }

    }

    public class MongoCursorIteration implements
    CloseableIteration<Namespace, RyaDAOException> {
        private final MongoCursor<Document> cursor;

        public MongoCursorIteration(final MongoCursor<Document> cursor) {
            this.cursor = cursor;
        }

        @Override
        public boolean hasNext() throws RyaDAOException {
            return cursor.hasNext();
        }

        @Override
        public Namespace next() throws RyaDAOException {
            final Document ns = cursor.next();
            final String namespace = (String) ns.get(NAMESPACE);
            final String prefix = (String) ns.get(PREFIX);

            final Namespace temp =  new NamespaceImplementation(namespace, prefix);
            return temp;
        }

        @Override
        public void remove() throws RyaDAOException {
            next();
        }

        @Override
        public void close() throws RyaDAOException {
            cursor.close();
        }

    }

    private static final String ID = "_id";
    private static final String PREFIX = "prefix";
    private static final String NAMESPACE = "namespace";
    private StatefulMongoDBRdfConfiguration conf;
    private final MongoCollection<Document> nsColl;


    public SimpleMongoDBNamespaceManager(final MongoCollection<Document> nameSpaceCollection) {
        nsColl = nameSpaceCollection;
    }

    @Override
    public void createIndices(final DBCollection coll){
        coll.createIndex(PREFIX);
        coll.createIndex(NAMESPACE);
    }

    @Override
    public void setConf(final StatefulMongoDBRdfConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public StatefulMongoDBRdfConfiguration getConf() {
        return conf;
    }

    @Override
    public void addNamespace(final String prefix, final String namespace)
            throws RyaDAOException {
        final String id = prefix;
        byte[] bytes = id.getBytes(StandardCharsets.UTF_8);
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-1");
            bytes = digest.digest(bytes);
        } catch (final NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        final Document doc = new Document(ID, new String(Hex.encodeHex(bytes)))
                .append(PREFIX, prefix)
                .append(NAMESPACE, namespace);
        nsColl.insertOne(doc);

    }

    @Override
    public String getNamespace(final String prefix) throws RyaDAOException {
        final Document query = new Document().append(PREFIX, prefix);
        final FindIterable<Document> iterable = nsColl.find(query);
        String nameSpace = prefix;
        try (final MongoCursor<Document> cursor = iterable.iterator()) {
            while (cursor.hasNext()){
                final Document obj = cursor.next();
                nameSpace = (String) obj.get(NAMESPACE);
            }
        }
        return nameSpace;
    }

    @Override
    public void removeNamespace(final String prefix) throws RyaDAOException {
        final Document query = new Document().append(PREFIX, prefix);
        nsColl.deleteMany(query);
    }

    @Override
    public CloseableIteration<? extends Namespace, RyaDAOException> iterateNamespace()
            throws RyaDAOException {
        final FindIterable<Document> iterable = nsColl.find();
        final MongoCursor<Document> cursor = iterable.iterator();
        return new MongoCursorIteration(cursor);
    }

}
