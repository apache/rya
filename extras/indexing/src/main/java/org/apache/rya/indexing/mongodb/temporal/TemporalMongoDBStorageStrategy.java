package org.apache.rya.indexing.mongodb.temporal;

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

import java.util.regex.Matcher;

import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.indexing.TemporalInstantRfc3339;
import org.apache.rya.indexing.TemporalInterval;
import org.apache.rya.indexing.mongodb.IndexingMongoDBStorageStrategy;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Defines how time based intervals/instants are stored in MongoDB.
 * <p>
 * Time can be stored as the following:
 * <p>l
 * <li><b>instant</b> {[statement], instant: TIME}</li>
 * <li><b>interval</b> {[statement], start: TIME, end: TIME}</li>
 * @see {@link TemporalInstantRfc3339} for how the dates are formatted.
 */
public class TemporalMongoDBStorageStrategy extends IndexingMongoDBStorageStrategy {
    public static final String INTERVAL_START = "start";
    public static final String INTERVAL_END = "end";
    public static final String INSTANT = "instant";

    @Override
    public void createIndices(final DBCollection coll){
        coll.createIndex(INTERVAL_START);
        coll.createIndex(INTERVAL_END);
        coll.createIndex(INSTANT);
    }

    @Override
    public DBObject serialize(final RyaStatement ryaStatement) {
        final BasicDBObject base = (BasicDBObject) super.serialize(ryaStatement);
        final DBObject time = getTimeValue(ryaStatement.getObject().getData());
        base.putAll(time.toMap());
        return base;
    }

    public DBObject getTimeValue(final String timeData) {
        final Matcher match = TemporalInstantRfc3339.PATTERN.matcher(timeData);
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();
        if(match.find()) {
            final TemporalInterval date = TemporalInstantRfc3339.parseInterval(timeData);
            builder.add(INTERVAL_START, date.getHasBeginning().getAsDateTime().toDate());
            builder.add(INTERVAL_END, date.getHasEnd().getAsDateTime().toDate());
        } else {
            builder.add(INSTANT, TemporalInstantRfc3339.FORMATTER.parseDateTime(timeData).toDate());
        }
        return builder.get();
    }


}