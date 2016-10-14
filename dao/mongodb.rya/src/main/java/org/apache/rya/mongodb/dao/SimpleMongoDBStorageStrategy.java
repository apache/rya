package org.apache.rya.mongodb.dao;

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

import static org.openrdf.model.vocabulary.XMLSchema.ANYURI;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.XMLSchema;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.query.RyaQuery;

/**
 * Defines how {@link RyaStatement}s are stored in MongoDB.
 */
public class SimpleMongoDBStorageStrategy implements MongoDBStorageStrategy<RyaStatement> {
    private static final Logger LOG = Logger.getLogger(SimpleMongoDBStorageStrategy.class);
    protected static final String ID = "_id";
    protected static final String OBJECT_TYPE = "objectType";
    protected static final String OBJECT_TYPE_VALUE = XMLSchema.ANYURI.stringValue();
    protected static final String CONTEXT = "context";
    protected static final String PREDICATE = "predicate";
    protected static final String OBJECT = "object";
    protected static final String SUBJECT = "subject";
    public static final String TIMESTAMP = "insertTimestamp";
    protected ValueFactoryImpl factory = new ValueFactoryImpl();

    @Override
    public void createIndices(final DBCollection coll){
        BasicDBObject doc = new BasicDBObject();
        doc.put(SUBJECT, 1);
        doc.put(PREDICATE, 1);
        coll.createIndex(doc);
        doc = new BasicDBObject(PREDICATE, 1);
        doc.put(OBJECT, 1);
        doc.put(OBJECT_TYPE, 1);
        coll.createIndex(doc);
        doc = new BasicDBObject(OBJECT, 1);
        doc = new BasicDBObject(OBJECT_TYPE, 1);
        doc.put(SUBJECT, 1);
        coll.createIndex(doc);
    }

    @Override
    public DBObject getQuery(final RyaStatement stmt) {
        final RyaURI subject = stmt.getSubject();
        final RyaURI predicate = stmt.getPredicate();
        final RyaType object = stmt.getObject();
        final RyaURI context = stmt.getContext();
        final BasicDBObject query = new BasicDBObject();
        if (subject != null){
            query.append(SUBJECT, subject.getData());
        }
        if (object != null){
            query.append(OBJECT, object.getData());
            query.append(OBJECT_TYPE, object.getDataType().toString());
        }
        if (predicate != null){
            query.append(PREDICATE, predicate.getData());
        }
        if (context != null){
            query.append(CONTEXT, context.getData());
        }

        return query;
    }

    @Override
    public RyaStatement deserializeDBObject(final DBObject queryResult) {
        final Map result = queryResult.toMap();
        final String subject = (String) result.get(SUBJECT);
        final String object = (String) result.get(OBJECT);
        final String objectType = (String) result.get(OBJECT_TYPE);
        final String predicate = (String) result.get(PREDICATE);
        final String context = (String) result.get(CONTEXT);
        final Long timestamp = (Long) result.get(TIMESTAMP);
        RyaType objectRya = null;
        if (objectType.equalsIgnoreCase(ANYURI.stringValue())){
            objectRya = new RyaURI(object);
        }
        else {
            objectRya = new RyaType(factory.createURI(objectType), object);
        }

        final RyaStatement statement;
        if (!context.isEmpty()){
            statement = new RyaStatement(new RyaURI(subject), new RyaURI(predicate), objectRya,
                    new RyaURI(context));
        } else {
            statement = new RyaStatement(new RyaURI(subject), new RyaURI(predicate), objectRya);
        }

        if(timestamp != null) {
            statement.setTimestamp(timestamp);
        }
        return statement;
    }

    @Override
    public DBObject serialize(final RyaStatement statement){
        return serializeInternal(statement);
    }

    public BasicDBObject serializeInternal(final RyaStatement statement){
        String context = "";
        if (statement.getContext() != null){
            context = statement.getContext().getData();
        }
        final String id = statement.getSubject().getData() + " " +
                statement.getPredicate().getData() + " " +  statement.getObject().getData() + " " + context;
        byte[] bytes = id.getBytes();
        try {
            final MessageDigest digest = MessageDigest.getInstance("SHA-1");
            bytes = digest.digest(bytes);
        } catch (final NoSuchAlgorithmException e) {
            LOG.error("Unable to perform SHA-1 on the ID, defaulting to raw bytes.", e);
        }
        final BasicDBObject doc = new BasicDBObject(ID, new String(Hex.encodeHex(bytes)))
        .append(SUBJECT, statement.getSubject().getData())
        .append(PREDICATE, statement.getPredicate().getData())
        .append(OBJECT, statement.getObject().getData())
        .append(OBJECT_TYPE, statement.getObject().getDataType().toString())
        .append(CONTEXT, context)
        .append(TIMESTAMP, statement.getTimestamp());
        return doc;

    }

    @Override
    public DBObject getQuery(final RyaQuery ryaQuery) {
        return getQuery(ryaQuery.getQuery());
    }
}
