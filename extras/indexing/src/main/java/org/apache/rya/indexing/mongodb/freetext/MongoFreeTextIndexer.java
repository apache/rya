package org.apache.rya.indexing.mongodb.freetext;
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

import java.io.IOException;

import org.apache.log4j.Logger;
import org.openrdf.model.Statement;
import org.openrdf.query.QueryEvaluationException;

import com.mongodb.QueryBuilder;

import info.aduna.iteration.CloseableIteration;
import org.apache.rya.indexing.FreeTextIndexer;
import org.apache.rya.indexing.StatementConstraints;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.mongodb.AbstractMongoIndexer;

public class MongoFreeTextIndexer extends AbstractMongoIndexer<TextMongoDBStorageStrategy> implements FreeTextIndexer {
    private static final String COLLECTION_SUFFIX = "freetext";
    private static final Logger logger = Logger.getLogger(MongoFreeTextIndexer.class);

    @Override
    public void init() {
        initCore();
        predicates = ConfigUtils.getFreeTextPredicates(conf);
        storageStrategy = new TextMongoDBStorageStrategy();
        storageStrategy.createIndices(collection);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryText(
            final String query, final StatementConstraints constraints) throws IOException {
        final QueryBuilder qb = QueryBuilder.start().text(query);
        return withConstraints(constraints, qb.get());
    }

    @Override
    public String getCollectionName() {
    	return ConfigUtils.getTablePrefix(conf)  + COLLECTION_SUFFIX;
    }
}
