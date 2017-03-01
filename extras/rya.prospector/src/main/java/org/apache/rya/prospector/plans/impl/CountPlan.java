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
package org.apache.rya.prospector.plans.impl;

import static org.apache.rya.prospector.utils.ProspectorConstants.COUNT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.prospector.domain.IndexEntry;
import org.apache.rya.prospector.domain.IntermediateProspect;
import org.apache.rya.prospector.domain.TripleValueType;
import org.apache.rya.prospector.plans.IndexWorkPlan;
import org.apache.rya.prospector.utils.CustomEntry;
import org.apache.rya.prospector.utils.ProspectorUtils;
import org.openrdf.model.util.URIUtil;
import org.openrdf.model.vocabulary.XMLSchema;

/**
 * An implementation of {@link IndexWorkPlan} that counts the number of times
 * a piece of data appears within a Rya Instance for every {@link TripleValueType}.
 */
public class CountPlan implements IndexWorkPlan {

    @Override
    public Collection<Map.Entry<IntermediateProspect, LongWritable>> map(RyaStatement ryaStatement) {
        final RyaURI subject = ryaStatement.getSubject();
        final RyaURI predicate = ryaStatement.getPredicate();
        final String subjpred = ryaStatement.getSubject().getData() + DELIM + ryaStatement.getPredicate().getData();
        final String predobj = ryaStatement.getPredicate().getData() + DELIM + ryaStatement.getObject().getData();
        final String subjobj = ryaStatement.getSubject().getData() + DELIM + ryaStatement.getObject().getData();
        final RyaType object = ryaStatement.getObject();
        final int localIndex = URIUtil.getLocalNameIndex(subject.getData());
        final String namespace = subject.getData().substring(0, localIndex - 1);
        final String visibility = new String(ryaStatement.getColumnVisibility());

        final List<Map.Entry<IntermediateProspect, LongWritable>> entries = new ArrayList<>(7);

        // Create an entry for each TripleValueType type.
        entries.add(new CustomEntry<IntermediateProspect, LongWritable>(
                IntermediateProspect.builder()
                    .setIndex(COUNT)
                    .setData(subject.getData())
                    .setDataType(URITYPE)
                    .setTripleValueType( TripleValueType.SUBJECT )
                    .setVisibility(visibility)
                    .build()
                , ONE));

        entries.add(new CustomEntry<IntermediateProspect, LongWritable>(
                IntermediateProspect.builder()
                    .setIndex(COUNT)
                    .setData(predicate.getData())
                    .setDataType(URITYPE)
                    .setTripleValueType( TripleValueType.PREDICATE )
                    .setVisibility(visibility)
                    .build()
                , ONE));

        entries.add(new CustomEntry<IntermediateProspect, LongWritable>(
                IntermediateProspect.builder()
                    .setIndex(COUNT)
                    .setData(object.getData())
                    .setDataType(object.getDataType().stringValue())
                    .setTripleValueType( TripleValueType.OBJECT )
                    .setVisibility(visibility)
                    .build()
                , ONE));

        entries.add(new CustomEntry<IntermediateProspect, LongWritable>(
                IntermediateProspect.builder()
                    .setIndex(COUNT)
                    .setData(subjpred)
                    .setDataType(XMLSchema.STRING.toString())
                    .setTripleValueType( TripleValueType.SUBJECT_PREDICATE )
                    .setVisibility(visibility)
                    .build()
                , ONE));

        entries.add(new CustomEntry<IntermediateProspect, LongWritable>(
                IntermediateProspect.builder()
                    .setIndex(COUNT)
                    .setData(subjobj)
                    .setDataType(XMLSchema.STRING.toString())
                    .setTripleValueType(TripleValueType.SUBJECT_OBJECT)
                    .setVisibility(visibility)
                    .build()
                , ONE));

        entries.add(new CustomEntry<IntermediateProspect, LongWritable>(
                IntermediateProspect.builder()
                    .setIndex(COUNT)
                    .setData(predobj)
                    .setDataType(XMLSchema.STRING.toString())
                    .setTripleValueType(TripleValueType.PREDICATE_OBJECT)
                    .setVisibility(visibility)
                    .build()
                , ONE));

        entries.add(new CustomEntry<IntermediateProspect, LongWritable>(
                IntermediateProspect.builder()
                    .setIndex(COUNT)
                    .setData(namespace)
                    .setDataType(URITYPE)
                    .setTripleValueType(TripleValueType.ENTITY)
                    .setVisibility(visibility)
                    .build()
                , ONE));
        return entries;
    }

    @Override
    public Collection<Map.Entry<IntermediateProspect, LongWritable>> combine(IntermediateProspect prospect, Iterable<LongWritable> counts) {
        long sum = 0;
        for(final LongWritable count : counts) {
            sum += count.get();
        }
        return Collections.singleton( new CustomEntry<IntermediateProspect, LongWritable>(prospect, new LongWritable(sum)) );
    }

    @Override
    public void reduce(IntermediateProspect prospect, Iterable<LongWritable> counts, Date timestamp, Reducer.Context context) throws IOException, InterruptedException {
        long sum = 0;
        for(final LongWritable count : counts) {
            sum += count.get();
        }

        final String indexType = prospect.getTripleValueType().getIndexType();

        // not sure if this is the best idea..
        if ((sum >= 0) || indexType.equals(TripleValueType.PREDICATE.getIndexType())) {
            final Mutation m = new Mutation(indexType + DELIM + prospect.getData() + DELIM + ProspectorUtils.getReverseIndexDateTime(timestamp));

            final String dataType = prospect.getDataType();
            final ColumnVisibility visibility = new ColumnVisibility(prospect.getVisibility());
            final Value sumValue = new Value(("" + sum).getBytes());
            m.put(COUNT, prospect.getDataType(), visibility, timestamp.getTime(), sumValue);

            context.write(null, m);
        }
    }

    @Override
    public String getIndexType() {
        return COUNT;
    }

    @Override
    public String getCompositeValue(List<String> indices){
        final Iterator<String> indexIt = indices.iterator();
        String compositeIndex = indexIt.next();
        while (indexIt.hasNext()){
            final String value = indexIt.next();
            compositeIndex += DELIM + value;
        }
        return compositeIndex;
    }

    @Override
    public List<IndexEntry> query(Connector connector, String tableName, List<Long> prospectTimes, String type, String compositeIndex, String dataType, String[] auths) throws TableNotFoundException {
        assert connector != null && tableName != null && type != null && compositeIndex != null;

        final BatchScanner bs = connector.createBatchScanner(tableName, new Authorizations(auths), 4);
        final List<Range> ranges = new ArrayList<>();
        int max = 1000; //by default only return 1000 prospects maximum
        if (prospectTimes != null) {
            for(final Long prospectTime : prospectTimes) {
                ranges.add(new Range(type + DELIM + compositeIndex + DELIM + ProspectorUtils.getReverseIndexDateTime(new Date(prospectTime))));
            }
        } else {
            max = 1; //only return the latest if no prospectTimes given
            final String prefix = type + DELIM + compositeIndex + DELIM;
            ranges.add(new Range(prefix, prefix + RdfCloudTripleStoreConstants.LAST));
        }

        bs.setRanges(ranges);
        if (dataType != null) {
            bs.fetchColumn(new Text(COUNT), new Text(dataType));
        } else {
            bs.fetchColumnFamily(new Text(COUNT));
        }

        final List<IndexEntry> indexEntries = new ArrayList<IndexEntry>();
        final Iterator<Entry<Key, Value>> iter = bs.iterator();

        while (iter.hasNext() && indexEntries.size() <= max) {
            final Entry<Key, Value> entry = iter.next();
            final Key k = entry.getKey();
            final Value v = entry.getValue();

            final String[] rowArr = k.getRow().toString().split(DELIM);
            String values = "";
            // if it is a composite index, then return the type as a composite index
            if (type.equalsIgnoreCase(TripleValueType.SUBJECT_PREDICATE.getIndexType()) ||
                type.equalsIgnoreCase(TripleValueType.SUBJECT_OBJECT.getIndexType()) ||
                type.equalsIgnoreCase(TripleValueType.PREDICATE_OBJECT.getIndexType())) {
                values =rowArr[1] + DELIM + rowArr[2];
            }
            else  {
                values = rowArr[1];
            }

            // Create an entry using the values that were found.
            final String entryDataType = k.getColumnQualifier().toString();
            final String entryVisibility = k.getColumnVisibility().toString();
            final Long entryCount = Long.parseLong(new String(v.get()));

            indexEntries.add(
                    IndexEntry.builder()
                        .setData(values)
                        .setTripleValueType(rowArr[0])
                        .setIndex(COUNT)
                        .setDataType(entryDataType)
                        .setVisibility(entryVisibility)
                        .setCount(entryCount)
                        .setTimestamp(k.getTimestamp())
                        .build());
        }
        bs.close();

        return indexEntries;
    }
}